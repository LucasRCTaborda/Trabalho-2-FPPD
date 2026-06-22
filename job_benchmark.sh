#!/bin/bash
#SBATCH --job-name=matmul_benchmark
#SBATCH --nodes=1
#SBATCH --ntasks=16
#SBATCH --time=16:00:00
#SBATCH --output=benchmark_%j.out

module load openmpi
module load go

REPO_DIR=$SLURM_SUBMIT_DIR
REPETICOES=3

echo "=============================================="
echo " BENCHMARK - Multiplicação de Matrizes N=3000"
echo " Data: $(date)"
echo " Nó: $(hostname)"
echo "=============================================="

# ── Compilar ──────────────────────────────────────
echo ""
echo "[1/2] Compilando versão sequencial..."
cd "$REPO_DIR/sequencial"
go build -o matmul_seq . && echo "  OK" || { echo "  ERRO na compilação sequencial"; exit 1; }

echo "[2/2] Compilando versão paralela..."
cd "$REPO_DIR/Paralelo"
go build -mod=vendor -o matmul_par . && echo "  OK" || { echo "  ERRO na compilação paralela"; exit 1; }

# ── Função para extrair tempo da saída ────────────
extrair_tempo() {
    grep "Tempo de execução" | grep -oP '[0-9]+\.[0-9]+'
}

# ── Função para calcular mediana de 3 valores ─────
mediana() {
    echo "$1 $2 $3" | tr ' ' '\n' | sort -n | sed -n '2p'
}

echo ""
echo "=============================================="
echo " EXECUTANDO TESTES (${REPETICOES}x cada config)"
echo "=============================================="

# ── Sequencial (baseline) ─────────────────────────
echo ""
echo ">>> CONFIG 1: Sequencial (1 processo)"
cd "$REPO_DIR/sequencial"
tempos_seq=()
for i in $(seq 1 $REPETICOES); do
    echo "  Execução $i/$REPETICOES..."
    t=$(./matmul_seq | extrair_tempo)
    tempos_seq+=($t)
    echo "    Tempo: ${t}s"
done
T_SEQ=$(mediana ${tempos_seq[0]} ${tempos_seq[1]} ${tempos_seq[2]})
echo "  Mediana: ${T_SEQ}s"

# ── Paralelo com N processos ──────────────────────
cd "$REPO_DIR/Paralelo"

for NP in 2 4 8 16; do
    echo ""
    echo ">>> CONFIG: Paralelo com ${NP} processos"
    tempos=()
    for i in $(seq 1 $REPETICOES); do
        echo "  Execução $i/$REPETICOES..."
        t=$(mpirun -np $NP ./matmul_par | extrair_tempo)
        tempos+=($t)
        echo "    Tempo: ${t}s"
    done
    T_MED=$(mediana ${tempos[0]} ${tempos[1]} ${tempos[2]})

    # Speedup e eficiência
    SPEEDUP=$(echo "scale=3; $T_SEQ / $T_MED" | bc)
    EFIC=$(echo "scale=3; $SPEEDUP / $NP * 100" | bc)

    echo "  Mediana: ${T_MED}s  |  Speedup: ${SPEEDUP}x  |  Eficiência: ${EFIC}%"
done

# ── Tabela final ──────────────────────────────────
echo ""
echo "=============================================="
echo " TABELA DE RESULTADOS"
echo "=============================================="
echo ""
echo "Nós | Processos | Tp (mediana) | Speedup | Eficiência"
echo "-----|-----------|--------------|---------|----------"

# Re-executa 1x cada config só para montar a tabela (já aquecido)
cd "$REPO_DIR/sequencial"
T=$(./matmul_seq | extrair_tempo)
echo "  1  |  1 (seq)  |   ${T}s   |  1.000  |  100.0%"
T_BASE=$T

cd "$REPO_DIR/Paralelo"
for NP in 2 4 8 16; do
    T=$(mpirun -np $NP ./matmul_par | extrair_tempo)
    SP=$(echo "scale=3; $T_BASE / $T" | bc)
    EF=$(echo "scale=1; $SP / $NP * 100" | bc)
    echo "  1  |    ${NP}     |   ${T}s   |  ${SP}  |  ${EF}%"
done

echo ""
echo "Benchmark concluído: $(date)"
