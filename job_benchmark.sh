#!/bin/bash
#SBATCH --job-name=matmul_benchmark
#SBATCH --nodes=1
#SBATCH --ntasks=16
#SBATCH --time=16:00:00
#SBATCH --output=benchmark_%j.out

# ── OpenMPI do cluster LAD/PUCRS ──────────────────
OMPI_DIR=/LADAPPs/OpenMPI/openmpi-4.1.1
export PATH=$OMPI_DIR/bin:$PATH
export LD_LIBRARY_PATH=$OMPI_DIR/lib:$LD_LIBRARY_PATH

mkdir -p ~/pkgconfig
cat > ~/pkgconfig/ompi.pc << PCEOF
Name: ompi
Description: Open MPI
Version: 4.1.1
Cflags: $(mpicc --showme:compile)
Libs: $(mpicc --showme:link)
PCEOF
export PKG_CONFIG_PATH=~/pkgconfig:$PKG_CONFIG_PATH

REPO_DIR=$SLURM_SUBMIT_DIR
REPETICOES=3

echo "=============================================="
echo " BENCHMARK — Multiplicação de Matrizes N=4000"
echo " Data: $(date)"
echo " Nó: $(hostname)"
echo " OpenMPI: $(mpirun --version 2>&1 | head -1)"
echo "=============================================="

# ── Compilar ───────────────────────────────────────
echo ""
echo "[1/2] Compilando versão sequencial..."
cd "$REPO_DIR/sequencial"
go build -o matmul_seq . && echo "  OK" || { echo "  ERRO sequencial"; exit 1; }

echo "[2/2] Compilando versão paralela..."
cd "$REPO_DIR/Paralelo"
go build -mod=vendor -o matmul_par . && echo "  OK" || { echo "  ERRO paralela"; exit 1; }

# ── Funções auxiliares ─────────────────────────────
extrair_tempo() {
    grep "Tempo de execução" | grep -oP '[0-9]+\.[0-9]+'
}

mediana() {
    echo "$1 $2 $3" | tr ' ' '\n' | sort -n | sed -n '2p'
}

echo ""
echo "=============================================="
echo " EXECUTANDO TESTES (${REPETICOES}x cada config)"
echo "=============================================="

# ── Config 1: Sequencial ──────────────────────────
echo ""
echo ">>> CONFIG 1: Sequencial (1 processo, 1 nó)"
cd "$REPO_DIR/sequencial"
t1=$(./matmul_seq | extrair_tempo); echo "  Exec 1: ${t1}s"
t2=$(./matmul_seq | extrair_tempo); echo "  Exec 2: ${t2}s"
t3=$(./matmul_seq | extrair_tempo); echo "  Exec 3: ${t3}s"
T_SEQ=$(mediana $t1 $t2 $t3)
echo "  Mediana: ${T_SEQ}s"

# ── Configs 2-5: Paralelo 1 nó (Fator 1 + 3) ─────
cd "$REPO_DIR/Paralelo"
for NP in 2 4 8 16; do
    echo ""
    echo ">>> CONFIG: Paralelo ${NP} processos, 1 nó"
    t1=$(mpirun -np $NP ./matmul_par | extrair_tempo); echo "  Exec 1: ${t1}s"
    t2=$(mpirun -np $NP ./matmul_par | extrair_tempo); echo "  Exec 2: ${t2}s"
    t3=$(mpirun -np $NP ./matmul_par | extrair_tempo); echo "  Exec 3: ${t3}s"
    T_MED=$(mediana $t1 $t2 $t3)
    SP=$(echo "scale=3; $T_SEQ / $T_MED" | bc)
    EF=$(echo "scale=1; $(echo "scale=4; $T_SEQ / $T_MED / $NP * 100" | bc)" | bc)
    echo "  Mediana: ${T_MED}s | Speedup: ${SP}x | Eficiência: ${EF}%"
done

echo ""
echo "=============================================="
echo " TABELA FINAL — FATOR 1 e FATOR 3"
echo "=============================================="
echo "Nós | Processos | Tp mediana (s) | Speedup | Eficiência"
echo "----|-----------|----------------|---------|----------"

cd "$REPO_DIR/sequencial"
TS=$(./matmul_seq | extrair_tempo)
echo "  1 |  1 (seq)  |     ${TS}s    |  1.000  |  100.0%"

cd "$REPO_DIR/Paralelo"
for NP in 2 4 8 16; do
    TP=$(mpirun -np $NP ./matmul_par | extrair_tempo)
    SP=$(echo "scale=3; $TS / $TP" | bc)
    EF=$(echo "scale=1; $(echo "scale=4; $TS / $TP / $NP * 100" | bc)" | bc)
    echo "  1 |    ${NP}      |     ${TP}s    |  ${SP}  |  ${EF}%"
done

echo ""
echo "Benchmark concluído: $(date)"
