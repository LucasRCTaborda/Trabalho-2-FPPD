# Trabalho-2-FPPD
 Processamento Paralelo: Multiplicação de Matrizes com MPI
# T2 — Multiplicação de Matrizes com MPI em Go

FPPD — Fundamentos de Processamento Paralelo e Distribuído (98713-04)  
Escola Politécnica — PUCRS — 2026/1

---

## Download de Dependencias

cd Paralelo
go mod tidy

## Estrutura do Projeto

```
t2/
├── sequencial/
│   ├── main.go       # Versão sequencial (baseline)
│   └── go.mod
├── paralelo/
│   ├── main.go       # Versão paralela com MPI (gompi)
│   └── go.mod
└── README.md
```

---

## Dependências

- Go 1.21+
- OpenMPI instalado no cluster (disponível via módulo)
- Pacote Go: `github.com/mnlphlp/gompi`

---

## Compilação e Execução Local (para testes)

### Versão Sequencial

```bash
cd Sequencial
go build -o matmul_seq .
./matmul_seq
```

### Versão Paralela

```bash
# Instalar OpenMPI (apenas uma vez, se ainda não instalado)
sudo apt install openmpi-bin libopenmpi-dev

cd Paralelo
go mod tidy        # baixa a dependência gompi
go build -o matmul_par .
mpirun -np 4 ./matmul_par
```

---

## Execução no Cluster Atlântica (SLURM)

### Carregar módulos necessários

```bash
module load openmpi
module load go
```

### Script SLURM — Versão Sequencial (baseline)

Salvar como `job_seq.sh`:

```bash
#!/bin/bash
#SBATCH --job-name=matmul_seq
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --time=16:00:00
#SBATCH --output=seq_%j.out

module load openmpi go

cd $SLURM_SUBMIT_DIR/sequencial
go build -o matmul_seq .

for i in 1 2 3; do
    echo "--- Execução $i ---"
    ./matmul_seq
done
```

### Script SLURM — Versão Paralela (exemplo: 4 processos em 1 nó)

Salvar como `job_par_1no_4p.sh`:

```bash
#!/bin/bash
#SBATCH --job-name=matmul_par_1n4p
#SBATCH --nodes=1
#SBATCH --ntasks=4
#SBATCH --time=08:00:00
#SBATCH --output=par_1n4p_%j.out

module load openmpi go

cd $SLURM_SUBMIT_DIR/paralelo
go build -o matmul_par .

for i in 1 2 3; do
    echo "--- Execução $i ---"
    mpirun -np 4 ./matmul_par
done
```

### Script SLURM — 4 processos em 2 nós (comparação inter-nós)

Salvar como `job_par_2nos_4p.sh`:

```bash
#!/bin/bash
#SBATCH --job-name=matmul_par_2n4p
#SBATCH --nodes=2
#SBATCH --ntasks=4
#SBATCH --ntasks-per-node=2
#SBATCH --time=08:00:00
#SBATCH --output=par_2n4p_%j.out

module load openmpi go

cd $SLURM_SUBMIT_DIR/paralelo
go build -o matmul_par .

for i in 1 2 3; do
    echo "--- Execução $i ---"
    mpirun -np 4 ./matmul_par
done
```

### Enviar jobs

```bash
sbatch job_seq.sh
sbatch job_par_1no_4p.sh
sbatch job_par_2nos_4p.sh
```

---

## Configurações Experimentais Planejadas (≥ 8 configurações)

| Config | Nós | Processos | Objetivo                              |
|--------|-----|-----------|---------------------------------------|
| 1      | 1   | 1 (seq)   | Baseline sequencial                   |
| 2      | 1   | 2         | Escalabilidade                        |
| 3      | 1   | 4         | Escalabilidade                        |
| 4      | 1   | 8         | Escalabilidade / Hyperthreading       |
| 5      | 1   | 16        | Oversubscription (hyperthreads)       |
| 6      | 2   | 4         | Comunicação inter-nós (Fator 2)       |
| 7      | 4   | 8         | Escalabilidade distribuída            |
| 8      | 8   | 16        | Alta escala distribuída               |

> Cada configuração é executada **3 vezes**; registrar a **mediana** dos tempos.

---

## Verificação de Corretude

Ambas as versões usam **seed = 42** para gerar as matrizes A e B.  
Os cantos de C e o checksum devem ser **idênticos** entre a versão sequencial e a paralela.

---

## Modelo de Paralelismo

**Mestre-Escravo com decomposição por linhas.**

- O rank 0 atua como mestre: aguarda os resultados dos demais e monta a matriz C final.
- Todos os processos geram A e B localmente (evita broadcast de O(N²) dados).
- Cada processo calcula um subconjunto de linhas de C.
- Os workers enviam suas linhas calculadas ao rank 0 via `Send/Recv`.

---

## Medição de Tempo

- `time.Now()` e `time.Since()` no rank 0.
- O tempo medido inclui: distribuição implícita (geração local) + cálculo + coleta (`Recv`).
- **Excluído**: geração das matrizes A e B e impressão de resultados.