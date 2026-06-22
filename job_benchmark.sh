#!/bin/bash
#SBATCH --job-name=matmul_benchmark
#SBATCH --nodes=1
#SBATCH --ntasks=16
#SBATCH --time=16:00:00
#SBATCH --output=benchmark_%j.out

# ── Configurar OpenMPI do cluster ──────────────────
OMPI_DIR=/LADAPPs/OpenMPI/openmpi-4.1.1
export PATH=$OMPI_DIR/bin:$PATH
export LD_LIBRARY_PATH=$OMPI_DIR/lib:$LD_LIBRARY_PATH

# Criar ompi.pc para o CGo do gompi conseguir compilar
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
echo " BENCHMARK - Multiplicação de Matrizes N=3000"
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

# ── Sequencial (baseline) ──────────────────────────
echo ""
echo ">>> SEQUENCIAL (1 processo)"
cd "$REPO_DIR/sequencial"
t1=$(./matmul_seq | extrair_tempo); echo "  Execução 1: ${t1}s"
t2=$(./matmul_seq | extrair_tempo); echo "  Execução 2: ${t2}s"
t3=$(./matmul_seq | extrair_tempo); echo "  Execução 3: ${t3}s"
T_SEQ=$(mediana $t1 $t2 $t3)
echo "  Mediana sequencial: ${T_SEQ}s"

# ── Paralelo por número de processos ──────────────
cd "$REPO_DIR/Paralelo"
for NP in 2 4 8 16; do
    echo ""
    echo ">>> PARALELO com ${NP} processos"
    t1=$(mpirun -np $NP ./matmul_par | extrair_tempo); echo "  Execução 1: ${t1}s"
    t2=$(mpirun -np $NP ./matmul_par | extrair_tempo); echo "  Execução 2: ${t2}s"
    t3=$(mpirun -np $NP ./matmul_par | extrair_tempo); echo "  Execução 3: ${t3}s"
    T_MED=$(mediana $t1 $t2 $t3)
    SPEEDUP=$(echo "scale=3; $T_SEQ / $T_MED" | bc)
    EFIC=$(echo "scale=1; $SPEEDUP / $NP * 100" | bc)
    echo "  Mediana: ${T_MED}s  |  Speedup: ${SPEEDUP}x  |  Eficiência: ${EFIC}%"
done

# ── Tabela resumo ──────────────────────────────────
echo ""
echo "=============================================="
echo " TABELA FINAL DE RESULTADOS"
echo "=============================================="
echo "Processos | Tp (mediana) | Speedup | Eficiência"
echo "----------|--------------|---------|----------"

cd "$REPO_DIR/sequencial"
TS=$(./matmul_seq | extrair_tempo)
echo "    1     |   ${TS}s   |  1.000  |  100.0%   (sequencial)"

cd "$REPO_DIR/Paralelo"
for NP in 2 4 8 16; do
    TP=$(mpirun -np $NP ./matmul_par | extrair_tempo)
    SP=$(echo "scale=3; $TS / $TP" | bc)
    EF=$(echo "scale=1; $SP / $NP * 100" | bc)
    echo "   ${NP}     |   ${TP}s   |  ${SP}  |  ${EF}%"
done

echo ""
echo "Benchmark concluído: $(date)"
