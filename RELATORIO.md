# T2 — Multiplicação de Matrizes com MPI em Go
**FPPD — Fundamentos de Processamento Paralelo e Distribuído (98713-04)**  
Escola Politécnica — PUCRS — 2026/1  
Cluster: Atlântica (LAD/PUCRS) — N = 4000

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
→ **N = 4000 é obrigatório.** Todos os experimentos a seguir foram executados com N = 4000.  
Tempo sequencial com N = 4000: **138.64 segundos**.

---

## 6. Resultados Obtidos (N = 4000)

**Ambiente:** Cluster Atlântica (LAD/PUCRS) — N = 4000  
**Datas:** 29/06/2026 (intra-nó e inter-nós)

### Tempos individuais — intra-nó (1 nó, `atlantica04`)

| Processos | Exec 1 (s) | Exec 2 (s) | Exec 3 (s) | Mediana (s) |
|-----------|------------|------------|------------|-------------|
| 1 (seq)   | 138.9006   | 138.4158   | 138.4114   | **138.4158** |
| 2         | 72.4917    | 72.0798    | 72.0819    | **72.0819**  |
| 4         | 37.1049    | 37.0832    | 36.4314    | **37.0832**  |
| 8         | 32.2039    | 32.2038    | 32.3772    | **32.2039**  |
| 16        | 33.0395    | 33.0267    | 32.9608    | **33.0267**  |

### Tempos individuais — inter-nós (2 nós, `atlantica03` + `atlantica04`)

| Processos | Exec 1 (s) | Exec 2 (s) | Exec 3 (s) | Mediana (s) |
|-----------|------------|------------|------------|-------------|
| 4 (2 nós) | 35.6432    | 36.6192    | 37.7636    | **36.6192**  |
| 8 (2 nós) | 24.8228    | 25.0956    | 27.8073    | **25.0956**  |

### Tabela de desempenho — Ts = 138.4158 s

| Nós | Processos | Tp mediana (s) | Speedup (Sp = Ts/Tp) | Eficiência (E = Sp/P) | Obs.                     |
|-----|-----------|----------------|----------------------|-----------------------|--------------------------|
| 1   | 1 (seq)   | 138.4158       | 1.000                | 100.0%                | Baseline                 |
| 1   | 2         | 72.0819        | 1.920                | 96.0%                 | Fator 1 — escalabilidade |
| 1   | 4         | 37.0832        | 3.732                | 93.3%                 | Fator 1 — escalabilidade |
| 1   | 8         | 32.2039        | 4.298                | 53.7%                 | Fator 1 + 3              |
| 1   | 16        | 33.0267        | 4.191                | 26.2%                 | Fator 3 — hyperthreading |
| 2   | 4         | 36.6192        | 3.780                | 94.5%                 | Fator 2 — inter-nós      |
| 2   | 8         | 25.0956        | 5.514                | 68.9%                 | Fator 2 — inter-nós      |

---

## 7. Gráficos

*(Ver arquivos `grafico_speedup.png`, `grafico_eficiencia.png` e `grafico_intra_inter.png` gerados pelo script `gerar_graficos.py`)*

### 7.1 Speedup vs. Número de Processos (N=4000, 1 nó)

```
Speedup
  16 |. . . . . . . . . . . . . . . . (ideal Sp = P)
     |
   8 |
     |
   4 |          * 3.79
     |               * 4.28  * 4.20
     |   * 1.92
     |
   1 |* 1.00
     +-----+-----+-----+------+----- Processos
     1     2     4     8     16

  * = speedup obtido     . = speedup ideal
```

### 7.2 Eficiência vs. Número de Processos (N=4000, 1 nó)

```
Eficiência
 100% |* 100%
      |  * 96.2%
  94% |     * 94.6%
      |
  53% |          * 53.5%
      |
  26% |               * 26.2%
      |
   0% +-----+-----+-----+------ Processos
      1     2     4     8    16

  * = eficiência obtida     --- = ideal (100%)
```

### 7.3 Comparação Intra-nó vs. Inter-nós (N=4000)

| Processos | 1 nó — intra (s) | 2 nós — inter (s) | Diferença |
|-----------|------------------|-------------------|-----------|
| 4         | 37.08            | 36.62             | −1.2%     |
| 8         | 32.20            | 25.10             | −22.0%    |

---

## 8. Discussão

### 8.1 O speedup é sub-linear, linear ou super-linear?

**Sub-linear.** O maior speedup intra-nó obtido foi **4.298x com 8 processos**, enquanto o ideal seria 8x. Causas:
- Com N=4000, cada processo mantém as matrizes A e B completas em memória (2 × 128 MB = 256 MB por processo). Com 8 processos em 1 nó, são ~2 GB apenas de dados de entrada, saturando o barramento de memória.
- Fração serial na coleta: o rank 0 recebe os blocos de cada worker sequencialmente via `Recv`.
- Com 16 processos ocorre oversubscription (mais processos do que cores físicos).

### 8.2 A partir de quantos processos a eficiência cai significativamente?

Entre **4 e 8 processos** a eficiência cai de **93.3% para 53.7%** — queda de ~40 pontos percentuais com N=4000. Essa queda é mais acentuada do que o esperado por overhead MPI puro e reflete a saturação da banda de memória: com 8 processos no mesmo nó, todos concorrem pelo mesmo controlador de memória para acessar a matriz B inteira (128 MB por processo).

### 8.3 Impacto da rede (intra-nó vs. inter-nós)

| Processos | 1 nó (s) | 2 nós (s) | Resultado |
|-----------|----------|-----------|-----------|
| 4         | 37.08    | 36.62     | Inter-nós **1.2% mais rápido** |
| 8         | 32.20    | 25.10     | Inter-nós **22.0% mais rápido** |

O resultado surpreendente é que com **8 processos, a execução inter-nós foi mais rápida** do que intra-nó. Isso se explica pelo gargalo de memória: com N=4000, cada processo precisa de 256 MB para A e B. Em 1 nó com 8 processos, os 8 processos disputam o mesmo controlador de memória (2 GB totais). Em 2 nós com 4 processos cada, essa pressão é dividida — cada nó tem apenas 4 × 256 MB = 1 GB acessando seu próprio controlador. O ganho com a memória distribuída supera o custo da comunicação inter-nós.

Com 4 processos, a diferença é mínima (1.2%) porque a pressão de memória ainda não é crítica em 1 nó — o benefício da distribuição de memória não compensa o overhead da rede nesse patamar.

### 8.4 Impacto do Hyperthreading

Comparando 8 processos (Sp = 4.298, E = 53.7%) com 16 processos (Sp = 4.191, E = 26.2%):

- Dobrar os processos gerou **speedup ligeiramente menor** (4.191 < 4.298).
- A eficiência caiu de 53.7% para 26.2% — praticamente metade.

**Conclusão:** hyperthreading **não é vantajoso** para esta aplicação. Com N=4000, a memória já é o gargalo com 8 processos; adicionar 8 hyperthreads a mais agrava a contenção sem trazer ganho de computação real, pois dois hyperthreads no mesmo core físico compartilham FPU e cache L1/L2.

### 8.5 Lei de Amdahl — Estimativa da fração paralelizável

```
f = (1/Sp - 1/P) / (1 - 1/P)
```

Usando P = 4, Sp = 3.732 (ponto com eficiência ainda alta, antes do gargalo de memória):

```
f = (1/Sp - 1/P) / (1 - 1/P)
f = (1/3.732 - 1/4) / (1 - 1/4)
f = (0.2679 - 0.25) / 0.75
f = 0.0179 / 0.75
f ≈ 0.024  →  2.4% serial
```

**Fração paralelizável: p = 1 - 0.024 ≈ 97.6%**

Speedup máximo teórico (P → ∞):
```
Sp_max = 1 / f = 1 / 0.024 ≈ 42x
```

Na prática o speedup é limitado muito antes desse valor pelo gargalo de memória observado a partir de 8 processos no mesmo nó.

---

## 9. Conclusões

| Quesito | Resultado |
|---------|-----------|
| N utilizado | **4000** (sequencial N=3000 = 58.5s < 3 min → N=4000 obrigatório) |
| Tempo sequencial (N=4000) | 138.64s |
| Melhor speedup intra-nó | 4.298x (8 processos, 1 nó) |
| Melhor speedup geral | **5.514x** (8 processos, 2 nós) |
| Melhor eficiência | 96.0% (2 processos) |
| Ponto ótimo intra-nó | **4 processos** (Sp=3.732, E=93.3%) |
| Ponto ótimo geral | **8 processos, 2 nós** (Sp=5.514, E=68.9%) |
| Fração paralelizável | ≈ 97.6% |
| Hyperthreading vantajoso? | Não — speedup cai de 4.298 para 4.191 ao dobrar processos |
| Inter-nós vs. intra-nó (4p) | Inter-nós **1.2% mais rápido** — diferença marginal, pressão de memória ainda baixa |
| Inter-nós vs. intra-nó (8p) | Inter-nós **22.0% mais rápido** — memória distribuída compensa custo de rede |

O modelo **Mestre-Escravo com decomposição por linhas** escala bem até 4 processos no mesmo nó (E=94.6%). Com N=4000, o gargalo de memória limita o ganho a partir de 8 processos intra-nó. A distribuição em múltiplos nós alivia esse gargalo — 8 processos em 2 nós superaram 8 processos em 1 nó, demonstrando que para este tamanho de problema a escalabilidade inter-nós é mais eficiente do que o hyperthreading.
