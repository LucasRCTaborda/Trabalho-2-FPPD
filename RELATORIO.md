# T2 — Multiplicação de Matrizes com MPI em Go
**FPPD — Fundamentos de Processamento Paralelo e Distribuído (98713-04)**  
Escola Politécnica — PUCRS — 2026/1  
Cluster: Atlântica (LAD/PUCRS) — N = 3000 *(ver Seção 5 sobre N=4000)*

---

## 1. Modelo de Paralelismo Escolhido

**Mestre-Escravo com decomposição por linhas (row-based decomposition).**

### Justificativa

A multiplicação `C = A × B` calcula cada linha `i` de `C` de forma independente:

```
C[i][j] = soma( A[i][k] * B[k][j] ), para k = 0..N-1
```

Como não há dependência entre linhas distintas de `C`, o trabalho pode ser dividido igualmente entre os processos. Cada processo calcula um bloco contíguo de linhas, sem comunicação durante o cálculo — apenas coleta final no rank 0.

---

## 2. Estratégia de Decomposição dos Dados

```
Processo 0  →  linhas 0        até (N/P - 1)
Processo 1  →  linhas N/P      até (2N/P - 1)
...
Processo P-1 → linhas restantes (inclui sobra se N % P ≠ 0)
```

**Decisão de projeto:** todos os processos geram A e B localmente com a mesma seed (42), eliminando o broadcast de O(N²) dados pela rede. Cada processo precisa de suas linhas de A e da matriz B inteira.

---

## 3. Explicação do Código

### 3.1 Versão Sequencial — `sequencial/Main.go`

```go
const SEED = 42

func main() {
    N := 4000  // N=3000 nos testes abaixo; aumentado para 4000 por exigência do trabalho

    // Slices 1D row-major: A[i][j] = A[i*N + j]
    A := make([]float64, N*N)
    B := make([]float64, N*N)
    C := make([]float64, N*N)

    rng := rand.New(rand.NewSource(SEED))
    for i := 0; i < N*N; i++ {
        A[i] = rng.Float64()
        B[i] = rng.Float64()
    }

    inicio := time.Now()  // timer inicia APÓS geração das matrizes

    // Loop (i, k, j): acesso sequencial a B[k*N+j] — melhor localidade de cache
    for i := 0; i < N; i++ {
        for k := 0; k < N; k++ {
            aik := A[i*N+k]
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

**Destaques:**
- Slices 1D (row-major) facilitam o envio MPI como bloco contíguo de memória.
- Ordem `(i, k, j)` no triplo loop: `aik` é carregado uma vez por iteração de `k`, e `B[k*N+j]` é acessado sequencialmente — melhor uso de cache L1/L2.
- Geração das matrizes **excluída** da medição de tempo.

---

### 3.2 Versão Paralela — `Paralelo/Main.go`

```go
func main() {
    mpi.Init()
    defer mpi.Finalize()

    comm := mpi.NewComm(true)  // panicOnErr=true
    rank := comm.GetRank()     // 0 = mestre
    size := comm.GetSize()     // total de processos

    N := 4000

    // Todos geram A e B com mesma seed — sem broadcast pela rede
    rng := rand.New(rand.NewSource(SEED))
    for i := 0; i < N*N; i++ {
        A[i] = rng.Float64()
        B[i] = rng.Float64()
    }

    // Divisão de linhas: primeiros (N%size) processos recebem 1 linha extra
    linhasPorProcesso := N / size
    linhaExtra        := N % size

    var startRow, numLinhas int
    if rank < linhaExtra {
        numLinhas = linhasPorProcesso + 1
        startRow  = rank * numLinhas
    } else {
        numLinhas = linhasPorProcesso
        startRow  = rank*numLinhas + linhaExtra
    }

    comm.Barrier()          // sincroniza todos antes de iniciar o timer

    var inicio time.Time
    if rank == 0 {
        inicio = time.Now() // timer inicia no rank 0 após barreira
    }

    // Computação local: cada processo calcula suas linhas de C
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

        for src := 1; src < size; src++ {
            // Recalcula startRow e numLinhas do processo src
            buf := make([]float64, srcLinhas*N)
            comm.Recv(buf, src, 0)
            copy(C[srcStart*N:(srcStart+srcLinhas)*N], buf)
        }

        tempo := time.Since(inicio)
        fmt.Printf("Tempo de execução paralela: %.4f segundos\n", tempo.Seconds())
        // imprime cantos e checksum

    } else {
        comm.Send(localC, 0, 0)  // worker envia suas linhas ao mestre
    }
}
```

**O tempo medido inclui:** cálculo paralelo + Send/Recv (coleta)  
**O tempo exclui:** geração das matrizes A e B

---

## 4. Como Compilar e Executar no Cluster (passo a passo)

### 4.1 Configurar OpenMPI (executar uma vez na sessão)

```bash
export PATH=/LADAPPs/OpenMPI/openmpi-4.1.1/bin:$PATH
export LD_LIBRARY_PATH=/LADAPPs/OpenMPI/openmpi-4.1.1/lib:$LD_LIBRARY_PATH

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
cd sequencial && go build -o matmul_seq .

# Paralelo (dependências embutidas no vendor/)
cd ../Paralelo && go build -mod=vendor -o matmul_par .
```

### 4.3 Submeter via SLURM

```bash
sbatch job_benchmark.sh   # Fator 1 e 3 — 1 nó, 2/4/8/16 processos
sbatch job_internos.sh    # Fator 2 — 2 nós, comparação inter-nós

squeue -u $USER           # acompanhar
cat benchmark_*.out       # ver resultados
cat internos_*.out
```

---

## 5. Tamanho do Problema

O trabalho exige N = 3000 como padrão. Se o tempo sequencial for inferior a 3 minutos, deve-se usar N = 4000.

**Resultado obtido no cluster:** tempo sequencial com N = 3000 foi **~58.5 segundos < 3 minutos**.  
→ **N = 4000 é obrigatório.** Os dados abaixo são de N = 3000 (primeira rodada). Os testes com N = 4000 estão em execução.

---

## 6. Resultados Obtidos (N = 3000 — rodada inicial)

**Ambiente:** Cluster Atlântica (LAD/PUCRS), 1 nó (`atlantica01`), N = 3000  
**Data:** 22/06/2026

### Tempos individuais — todas as execuções

| Processos | Exec 1 (s) | Exec 2 (s) | Exec 3 (s) | Mediana (s) |
|-----------|-----------|-----------|-----------|-------------|
| 1 (seq)   | 58.5334   | 58.4252   | 58.4752   | **58.4752** |
| 2         | 30.3831   | 30.4031   | 30.2317   | **30.3831** |
| 4         | 15.5355   | 15.5107   | 15.3565   | **15.5107** |
| 8         | 8.9410    | 8.9217    | 8.9421    | **8.9410**  |
| 16        | 8.6308    | 8.6386    | 8.6456    | **8.6386**  |

### Tabela de desempenho — Ts = 58.4752 s

| Nós | Processos | Tp mediana (s) | Speedup (Sp = Ts/Tp) | Eficiência (E = Sp/P) | Obs.                    |
|-----|-----------|----------------|----------------------|-----------------------|-------------------------|
| 1   | 1 (seq)   | 58.4752        | 1.000                | 100.0%                | Baseline                |
| 1   | 2         | 30.3831        | 1.924                | 96.2%                 | Fator 1 — escalabilidade |
| 1   | 4         | 15.5107        | 3.769                | 94.2%                 | Fator 1 — escalabilidade |
| 1   | 8         | 8.9410         | 6.540                | 81.8%                 | Fator 1 + 3             |
| 1   | 16        | 8.6386         | 6.769                | 42.3%                 | Fator 3 — hyperthreading |
| 2   | 4         | *pendente*     | —                    | —                     | Fator 2 — inter-nós     |
| 2   | 8         | *pendente*     | —                    | —                     | Fator 2 — inter-nós     |

---

## 7. Gráficos

### 7.1 Speedup vs. Número de Processos

```
Speedup
  16 |. . . . . . . . . . . . . . . . (ideal Sp = P)
     |
   8 |               * 6.54   * 6.77
     |          * 3.77
     |
   4 |
     |   * 1.92
     |
   1 |* 1.00
     +-----+-----+-----+------+----- Processos
     1     2     4     8     16

  * = speedup obtido     . = speedup ideal
```

### 7.2 Eficiência vs. Número de Processos

```
Eficiência
 100% |* 100%
      |  * 96.2%
  94% |     * 94.2%
      |
  81% |          * 81.8%
      |
  42% |               * 42.3%
      |
   0% +-----+-----+-----+------ Processos
      1     2     4     8    16

  * = eficiência obtida     --- = ideal (100%)
```

### 7.3 Comparação Intra-nó vs. Inter-nós

*Dados inter-nós pendentes (job_internos.sh em execução). Tabela será completada após resultados.*

| Processos | 1 nó (intra) | 2 nós (inter) | Diferença |
|-----------|-------------|---------------|-----------|
| 4         | 15.51s      | *pendente*    | —         |
| 8         | 8.94s       | *pendente*    | —         |

---

## 8. Discussão

### 8.1 O speedup é sub-linear, linear ou super-linear?

**Sub-linear.** O maior speedup obtido foi **6.769x com 16 processos**, enquanto o ideal seria 16x. Causas:
- Fração serial na coleta de resultados: o rank 0 recebe sequencialmente de cada worker via `Recv`.
- Overhead de comunicação MPI (Send/Recv) cresce com o número de processos.
- Com 16 processos em 1 nó, ocorre oversubscription (mais processos do que cores físicos).

### 8.2 A partir de quantos processos a eficiência cai significativamente?

Entre **8 e 16 processos** a eficiência despenca de **81.8% para 42.3%** — queda de ~40 pontos percentuais. Causa: o nó não tem 16 cores físicos. Com oversubscription, os processos disputam as mesmas unidades de execução (FPU, cache L1/L2, barramento de memória), gerando contenção sem ganho proporcional.

### 8.3 Impacto da rede (intra-nó vs. inter-nós)

*A ser completado após execução do `job_internos.sh`.*

A expectativa teórica é que execuções inter-nós sejam mais lentas do que intra-nó com o mesmo número de processos, pois:
- Comunicação intra-nó usa memória compartilhada ou InfiniBand local (baixa latência).
- Comunicação inter-nós passa pela rede Ethernet/InfiniBand do cluster, com maior latência e menor banda efetiva para mensagens grandes (blocos de linhas de N=4000 float64).

### 8.4 Impacto do Hyperthreading

Comparando 8 processos (Sp = 6.540, E = 81.8%) com 16 processos (Sp = 6.769, E = 42.3%):

- Dobrar os processos gerou apenas **+0.23 de speedup adicional**.
- A eficiência caiu pela metade.

**Conclusão:** hyperthreading **não é vantajoso** para esta aplicação. Multiplicação de matrizes é intensiva em FPU e memória — dois hyperthreads no mesmo core físico compartilham as mesmas unidades de execução, gerando contenção em vez de paralelismo real.

### 8.5 Lei de Amdahl — Estimativa da fração paralelizável

```
Sp = 1 / (f + (1 - f) / P)
```

Usando P = 8, Sp = 6.540:

```
6.540 = 1 / (f + (1-f)/8)
6.540 × (8f + 1 - f) / 8 = 1
6.540 × (7f + 1) = 8
45.78f + 6.540 = 8
45.78f = 1.460
f ≈ 0.032  →  3.2% serial
```

**Fração paralelizável: p = 1 - 0.032 ≈ 96.8%**

Speedup máximo teórico (P → ∞):
```
Sp_max = 1 / f = 1 / 0.032 ≈ 31.3x
```

---

## 9. Conclusões

| Quesito | Resultado |
|---------|-----------|
| N utilizado | 3000 (rodada inicial); **4000 obrigatório** |
| Tempo sequencial (N=3000) | 58.48s — inferior a 3 min → exige N=4000 |
| Melhor speedup obtido | 6.769x (16 processos, 1 nó) |
| Melhor eficiência | 96.2% (2 processos) |
| Ponto ótimo prático | **8 processos** (Sp=6.54, E=81.8%) |
| Fração paralelizável | ≈ 96.8% |
| Hyperthreading vantajoso? | Não — ganho marginal com custo alto de eficiência |
| Dados inter-nós | Pendente (job_internos.sh) |

O modelo **Mestre-Escravo com decomposição por linhas** é eficiente até 8 processos no mesmo nó. Acima disso, o overhead de oversubscription domina. Para escalar além de 8 processos com boa eficiência, é necessário distribuir em múltiplos nós — o que será avaliado nos experimentos inter-nós.
