# T2 — Multiplicação de Matrizes com MPI em Go
**FPPD — Fundamentos de Processamento Paralelo e Distribuído (98713-04)**  
Escola Politécnica — PUCRS — 2026/1  
Cluster: Atlântica (LAD/PUCRS) — N = 3000

---

## 1. Modelo de Paralelismo Escolhido

**Mestre-Escravo com decomposição por linhas (row-based decomposition).**

### Justificativa

A multiplicação de matrizes `C = A × B` calcula cada linha `i` de `C` de forma independente:

```
C[i][j] = soma de A[i][k] * B[k][j], para k = 0..N-1
```

Como não há dependência entre linhas diferentes de `C`, o trabalho pode ser dividido igualmente entre os processos. Cada processo calcula um bloco contíguo de linhas de `C`, sem precisar se comunicar com outros processos durante o cálculo — apenas no início (distribuição) e no fim (coleta).

O rank 0 atua como mestre: sincroniza o início, aguarda os resultados e monta a matriz `C` final.

---

## 2. Estratégia de Decomposição dos Dados

```
Processo 0: linhas 0   .. N/P - 1
Processo 1: linhas N/P .. 2N/P - 1
...
Processo P-1: linhas restantes (inclui sobra se N % P != 0)
```

**Evitamos broadcast de A e B:** todos os processos geram as matrizes localmente com a mesma seed (42), eliminando O(N²) de dados na rede.

**Cada processo precisa de:**
- Suas linhas de A (para calcular)
- A matriz B completa (cada linha de C depende de toda B)

---

## 3. Explicação do Código

### 3.1 Versão Sequencial (`sequencial/Main.go`)

```go
const SEED = 42

func main() {
    N := 3000

    // Matrizes como slices 1D row-major: A[i][j] = A[i*N + j]
    A := make([]float64, N*N)
    B := make([]float64, N*N)
    C := make([]float64, N*N)

    // Geração determinística — mesma seed garante reprodutibilidade
    rng := rand.New(rand.NewSource(SEED))
    for i := 0; i < N*N; i++ {
        A[i] = rng.Float64()
        B[i] = rng.Float64()
    }

    inicio := time.Now()

    // Algoritmo ingênuo (i, k, j) — ordem k antes de j melhora cache
    for i := 0; i < N; i++ {
        for k := 0; k < N; k++ {
            aik := A[i*N+k]       // carrega A[i][k] uma vez fora do loop j
            for j := 0; j < N; j++ {
                C[i*N+j] += aik * B[k*N+j]
            }
        }
    }

    tempo := time.Since(inicio)
    fmt.Printf("Tempo de execução sequencial: %.4f segundos\n", tempo.Seconds())

    // Verificação: cantos e checksum devem ser idênticos ao paralelo
    fmt.Printf("C[0][0]     = %.6f\n", C[0])
    fmt.Printf("C[0][N-1]   = %.6f\n", C[N-1])
    fmt.Printf("C[N-1][0]   = %.6f\n", C[(N-1)*N])
    fmt.Printf("C[N-1][N-1] = %.6f\n", C[(N-1)*N+(N-1)])
}
```

**Pontos importantes:**
- Slices 1D (`row-major`) facilitam o envio por MPI (bloco contíguo de memória).
- O loop interno é `(i, k, j)` em vez de `(i, j, k)` para melhor localidade de cache: `B[k*N+j]` é acessado sequencialmente no loop mais interno.
- A geração das matrizes é **excluída** da medição de tempo.

---

### 3.2 Versão Paralela (`Paralelo/Main.go`)

```go
func main() {
    mpi.Init()
    defer mpi.Finalize()

    comm := mpi.NewComm(true)   // panicOnErr=true: encerra em erro MPI
    rank := comm.GetRank()      // identificador do processo (0 = mestre)
    size := comm.GetSize()      // total de processos

    N := 3000

    // Todos os processos geram A e B com a mesma seed
    // Evita broadcast de O(N²) dados pela rede
    A := make([]float64, N*N)
    B := make([]float64, N*N)
    rng := rand.New(rand.NewSource(SEED))
    for i := 0; i < N*N; i++ {
        A[i] = rng.Float64()
        B[i] = rng.Float64()
    }

    // Divisão de linhas entre processos
    linhasPorProcesso := N / size
    linhaExtra := N % size      // sobra distribuída nos primeiros processos

    var startRow, numLinhas int
    if rank < linhaExtra {
        numLinhas = linhasPorProcesso + 1
        startRow  = rank * numLinhas
    } else {
        numLinhas = linhasPorProcesso
        startRow  = rank*numLinhas + linhaExtra
    }

    // Sincronização antes de medir o tempo
    comm.Barrier()

    var inicio time.Time
    if rank == 0 {
        inicio = time.Now()    // timer inicia ANTES de qualquer impressão
    }

    // Cálculo local: cada processo computa suas linhas de C
    localC := make([]float64, numLinhas*N)
    for i := 0; i < numLinhas; i++ {
        globalI := startRow + i
        for k := 0; k < N; k++ {
            aik := A[globalI*N+k]
            for j := 0; j < N; j++ {
                localC[i*N+j] += aik * B[k*N+j]
            }
        }
    }

    // Coleta no rank 0
    if rank == 0 {
        C := make([]float64, N*N)
        copy(C[startRow*N:(startRow+numLinhas)*N], localC)

        // Receber de cada worker
        for src := 1; src < size; src++ {
            var srcStart, srcLinhas int
            if src < linhaExtra {
                srcLinhas = linhasPorProcesso + 1
                srcStart  = src * srcLinhas
            } else {
                srcLinhas = linhasPorProcesso
                srcStart  = src*srcLinhas + linhaExtra
            }
            buf := make([]float64, srcLinhas*N)
            comm.Recv(buf, src, 0)
            copy(C[srcStart*N:(srcStart+srcLinhas)*N], buf)
        }

        tempo := time.Since(inicio)
        fmt.Printf("Tempo de execução paralela: %.4f segundos\n", tempo.Seconds())
        // ... impressão dos cantos e checksum

    } else {
        // Workers enviam suas linhas ao mestre
        comm.Send(localC, 0, 0)
    }
}
```

**O tempo medido inclui:** cálculo paralelo + comunicação Send/Recv  
**O tempo exclui:** geração das matrizes A e B

---

## 4. Como Compilar e Executar (passo a passo)

### 4.1 Pré-requisitos no cluster

```bash
# Configurar OpenMPI (LAD/PUCRS)
export PATH=/LADAPPs/OpenMPI/openmpi-4.1.1/bin:$PATH
export LD_LIBRARY_PATH=/LADAPPs/OpenMPI/openmpi-4.1.1/lib:$LD_LIBRARY_PATH

# Criar pkg-config para o compilador Go encontrar o OpenMPI
mkdir -p ~/pkgconfig
cat > ~/pkgconfig/ompi.pc << EOF
Name: ompi
Description: Open MPI
Version: 4.1.1
Cflags: $(mpicc --showme:compile)
Libs: $(mpicc --showme:link)
EOF
export PKG_CONFIG_PATH=~/pkgconfig:$PKG_CONFIG_PATH
```

### 4.2 Clonar e compilar

```bash
git clone https://github.com/LucasRCTaborda/Trabalho-2-FPPD.git
cd Trabalho-2-FPPD

# Sequencial
cd sequencial
go build -o matmul_seq .

# Paralelo (usa vendor — sem internet necessária)
cd ../Paralelo
go mod vendor          # apenas se vendor/ não existir
go build -mod=vendor -o matmul_par .
```

### 4.3 Executar via SLURM (obrigatório para benchmarks)

```bash
# Submeter o benchmark completo (1, 2, 4, 8, 16 processos — 3x cada)
sbatch job_benchmark.sh

# Acompanhar
squeue -u $USER

# Ver resultado ao terminar
cat benchmark_*.out
```

> **Nunca rodar `mpirun` diretamente no nó de login** — use sempre `sbatch` ou `salloc`.

---

## 5. Resultados Obtidos

**Ambiente:** Cluster Atlântica (LAD/PUCRS), 1 nó (`atlantica01`), N = 3000  
**Data:** 22/06/2026

### Tempos individuais (3 execuções cada)

| Processos | Exec 1 (s) | Exec 2 (s) | Exec 3 (s) | Mediana (s) |
|-----------|-----------|-----------|-----------|-------------|
| 1 (seq)   | 58.5334   | 58.4252   | 58.4752   | **58.4752** |
| 2         | 30.3831   | 30.4031   | 30.2317   | **30.3831** |
| 4         | 15.5355   | 15.5107   | 15.3565   | **15.5107** |
| 8         | 8.9410    | 8.9217    | 8.9421    | **8.9410**  |
| 16        | 8.6308    | 8.6386    | 8.6456    | **8.6386**  |

### Tabela de desempenho

Ts (baseline) = **58.4752 s**

| Nós | Processos | Tp mediana (s) | Speedup (Sp = Ts/Tp) | Eficiência (E = Sp/P) | Obs.               |
|-----|-----------|----------------|----------------------|-----------------------|--------------------|
| 1   | 1 (seq)   | 58.4752        | 1.000                | 100.0%                | Baseline           |
| 1   | 2         | 30.3831        | 1.924                | 96.2%                 | Escalabilidade     |
| 1   | 4         | 15.5107        | 3.769                | 94.2%                 | Escalabilidade     |
| 1   | 8         | 8.9410         | 6.540                | 81.7%                 | Próx. de cores físicos |
| 1   | 16        | 8.6386         | 6.769                | 42.3%                 | Oversubscription   |

---

## 6. Gráficos

### 6.1 Speedup vs. Número de Processos

```
Speedup
  16 |. . . . . . . . . . . . . . . (ideal)
     |                          /
   8 |               . . . . ./. .
     |          *6.54         /
     |      *3.77            /
   4 |                      /
     |   *1.92             /
     |                    /
   1 |*1.00              /
     +----+----+----+----+----- Processos
     1    2    4    8   16

* = speedup obtido   / = speedup ideal (Sp = P)
```

### 6.2 Eficiência vs. Número de Processos

```
Eficiência
 100%|*----*
     |      \*
  80%|        \*
     |
  42%|            *
     |
   0%+----+----+----+---- Processos
     2    4    8   16

* = eficiência obtida   --- = ideal (100%)
```

### 6.3 Observação sobre inter-nós

Não foram coletados dados inter-nós nesta rodada (apenas 1 nó).  
Para completar o Fator 2 do trabalho, submeter com `--nodes=2 --ntasks=4 --ntasks-per-node=2` e comparar com a execução de 4 processos em 1 nó.

---

## 7. Discussão

### 7.1 O speedup é sub-linear, linear ou super-linear?

**Sub-linear.** O maior speedup obtido foi **6.77x com 16 processos**, quando o ideal seria 16x. Isso é esperado porque:
- Existe uma fração serial na aplicação (gerenciamento, Send/Recv no rank 0).
- O overhead de comunicação (Send/Recv) aumenta com o número de processos.
- Com 16 processos em 1 nó, ocorre oversubscription (mais processos do que cores físicos).

### 7.2 A partir de quantos processos a eficiência cai significativamente?

Entre **8 e 16 processos** a eficiência cai de **81.7% para 42.3%** — uma queda de quase 50 pontos percentuais. A causa provável é o **oversubscription**: o nó não possui 16 cores físicos. Com mais processos do que cores, o SO usa hyperthreading e os processos disputam recursos de CPU, memória cache e barramento de memória, aumentando o overhead sem ganho proporcional de computação.

### 7.3 Impacto da rede (intra-nó vs. inter-nós)

Todos os testes foram executados em 1 nó (comunicação via memória compartilhada/InfiniBand intra-nó). A expectativa para execução inter-nós é que o tempo aumente devido à latência de rede, especialmente na coleta de resultados pelo rank 0 (múltiplos `Recv` de nós distintos).

Para completar esta análise, executar com `--nodes=2 --ntasks=4 --ntasks-per-node=2` e comparar com `--nodes=1 --ntasks=4`.

### 7.4 Impacto do Hyperthreading

Comparando 8 processos (Sp = 6.540) com 16 processos (Sp = 6.769):
- O ganho de speedup foi de apenas **0.23x** ao dobrar os processos.
- A eficiência caiu pela metade (de 81.7% para 42.3%).

**Conclusão:** o hyperthreading **não é vantajoso** para esta aplicação. Multiplicação de matrizes é intensiva em memória e FPU — dois hyperthreads no mesmo core físico disputam as mesmas unidades de execução, gerando contenção em vez de paralelismo real.

### 7.5 Lei de Amdahl — Estimativa da fração paralelizável

A Lei de Amdahl diz:

```
Sp = 1 / (f + (1 - f) / P)
```

Onde `f` é a fração serial e `P` é o número de processos. Usando os dados de 8 processos (Sp = 6.540):

```
6.540 = 1 / (f + (1 - f) / 8)
6.540 × (f + (1-f)/8) = 1
6.540f + 0.8175(1-f) = 1
5.7225f = 0.1825
f ≈ 0.032 (3.2%)
```

**Fração paralelizável: p = 1 - f ≈ 96.8%**

Isso significa que aproximadamente **3.2% do tempo** é inerentemente serial (inicialização MPI, geração de matrizes antes do timer não foi contada, coleta sequencial no rank 0). Com infinitos processos, o speedup máximo teórico seria:

```
Sp_max = 1 / f = 1 / 0.032 ≈ 31.3x
```

---

## 8. Conclusões

| Quesito | Resultado |
|---------|-----------|
| Melhor speedup obtido | 6.769x (16 processos, 1 nó) |
| Melhor eficiência | 96.2% (2 processos) |
| Ponto ótimo prático | 8 processos (Sp=6.54, E=81.7%) |
| Fração paralelizável | ≈ 96.8% |
| Hyperthreading vantajoso? | Não — ganho marginal com alto custo de eficiência |

O modelo **Mestre-Escravo com decomposição por linhas** se mostrou eficiente para até 8 processos no mesmo nó. A partir de 16 processos, o overhead de oversubscription supera o ganho paralelo. Para escalar além de 8 processos com boa eficiência, seria necessário distribuir em múltiplos nós.
