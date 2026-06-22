# T2 — Multiplicação de Matrizes com MPI em Go

FPPD — Fundamentos de Processamento Paralelo e Distribuído (98713-04)  
Escola Politécnica — PUCRS — 2026/1

---

## Estrutura do Projeto

```
Trabalho-2-FPPD/
├── sequencial/
│   ├── Main.go       # Versão sequencial (baseline)
│   └── go.mod
├── Paralelo/
│   ├── Main.go       # Versão paralela com MPI (gompi)
│   ├── go.mod
│   ├── go.sum
│   └── vendor/       # Dependências embutidas (não precisa de internet no cluster)
└── README.md
```

---

## Dependências

- Go 1.21+
- OpenMPI (disponível via `module load openmpi` no cluster)
- Pacote Go: `github.com/mnlphlp/gompi` (já embutido em `Paralelo/vendor/`)

---

## Conexão SSH ao Cluster Atlântica (PUCRS)

### 1. Gerar chave SSH (se ainda não tiver)

```bash
ssh-keygen -t ed25519 -C "seu_email@edu.pucrs.br"
# Pressione Enter para aceitar o caminho padrão (~/.ssh/id_ed25519)
```

### 2. Copiar chave para o cluster

```bash
ssh-copy-id USUARIO@atlantica.lad.pucrs.br
# substitua USUARIO pelo seu login da PUCRS
```

### 3. Conectar

```bash
ssh USUARIO@atlantica.lad.pucrs.br
```

### 4. Clonar o repositório no cluster

```bash
git clone https://github.com/LucasRCTaborda/Trabalho-2-FPPD.git
cd Trabalho-2-FPPD
```

---

## Compilação no Cluster

### Carregar módulos

```bash
module load openmpi
module load go
```

### Versão Sequencial

```bash
cd sequencial
go build -o matmul_seq .
./matmul_seq
```

### Versão Paralela (usa vendor — sem internet necessária)

```bash
cd Paralelo
go build -mod=vendor -o matmul_par .
mpirun -np 4 ./matmul_par
```

---

## Scripts SLURM

### Sequencial (baseline) — `job_seq.sh`

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

### Paralela — 1 nó, 4 processos — `job_par_1n4p.sh`

```bash
#!/bin/bash
#SBATCH --job-name=matmul_par_1n4p
#SBATCH --nodes=1
#SBATCH --ntasks=4
#SBATCH --time=08:00:00
#SBATCH --output=par_1n4p_%j.out

module load openmpi go

cd $SLURM_SUBMIT_DIR/Paralelo
go build -mod=vendor -o matmul_par .

for i in 1 2 3; do
    echo "--- Execução $i ---"
    mpirun -np 4 ./matmul_par
done
```

### Paralela — 2 nós, 4 processos — `job_par_2n4p.sh`

```bash
#!/bin/bash
#SBATCH --job-name=matmul_par_2n4p
#SBATCH --nodes=2
#SBATCH --ntasks=4
#SBATCH --ntasks-per-node=2
#SBATCH --time=08:00:00
#SBATCH --output=par_2n4p_%j.out

module load openmpi go

cd $SLURM_SUBMIT_DIR/Paralelo
go build -mod=vendor -o matmul_par .

for i in 1 2 3; do
    echo "--- Execução $i ---"
    mpirun -np 4 ./matmul_par
done
```

### Paralela — 1 nó, 8 processos — `job_par_1n8p.sh`

```bash
#!/bin/bash
#SBATCH --job-name=matmul_par_1n8p
#SBATCH --nodes=1
#SBATCH --ntasks=8
#SBATCH --time=08:00:00
#SBATCH --output=par_1n8p_%j.out

module load openmpi go

cd $SLURM_SUBMIT_DIR/Paralelo
go build -mod=vendor -o matmul_par .

for i in 1 2 3; do
    echo "--- Execução $i ---"
    mpirun -np 8 ./matmul_par
done
```

### Enviar jobs

```bash
sbatch job_seq.sh
sbatch job_par_1n4p.sh
sbatch job_par_2n4p.sh
sbatch job_par_1n8p.sh
# etc.
```

### Verificar fila

```bash
squeue -u $USER
```

### Ver resultado

```bash
cat seq_*.out
cat par_1n4p_*.out
```

---

## Configurações Experimentais (≥ 8 configurações)

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

> Cada configuração executada **3 vezes** — registrar a **mediana** dos tempos.

---

## Verificação de Corretude

Ambas as versões usam **seed = 42**. Os cantos de C e o checksum devem ser **idênticos**.

---

## Modelo de Paralelismo

**Mestre-Escravo com decomposição por linhas.**

- Todos os processos geram A e B localmente (evita broadcast de O(N²) dados).
- Cada processo calcula um subconjunto de linhas de C.
- Workers enviam suas linhas ao rank 0 via `Send/Recv`.
- Rank 0 monta C completo e mede o tempo total.

---

## Medição de Tempo

- Timer inicia no rank 0 **após** `Barrier()` (todos sincronizados) e **antes** do cálculo.
- Inclui: cálculo + coleta (`Recv`).
- Exclui: geração das matrizes A e B.
