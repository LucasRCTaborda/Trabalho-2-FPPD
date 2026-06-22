#!/bin/bash
#SBATCH --job-name=matmul_internos
#SBATCH --nodes=2
#SBATCH --ntasks=8
#SBATCH --ntasks-per-node=4
#SBATCH --time=08:00:00
#SBATCH --output=internos_%j.out

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
echo " FATOR 2 — Comunicação via Rede (N=4000)"
echo " Data: $(date)"
echo " Nós alocados: $SLURM_JOB_NUM_NODES"
echo " Nós: $SLURM_JOB_NODELIST"
echo "=============================================="

# ── Compilar ───────────────────────────────────────
echo ""
echo "Compilando versão paralela..."
cd "$REPO_DIR/Paralelo"
go build -mod=vendor -o matmul_par . && echo "  OK" || { echo "  ERRO"; exit 1; }

extrair_tempo() {
    grep "Tempo de execução" | grep -oP '[0-9]+\.[0-9]+'
}

mediana() {
    echo "$1 $2 $3" | tr ' ' '\n' | sort -n | sed -n '2p'
}

echo ""
echo "=============================================="
echo " EXECUTANDO — FATOR 2 (intra-nó vs inter-nós)"
echo "=============================================="

cd "$REPO_DIR/Paralelo"

# ── Config 6: 4 processos, 2 nós (2 por nó) ──────
echo ""
echo ">>> CONFIG 6: 4 processos, 2 nós (2 por nó)"
t1=$(mpirun -np 4 --map-by node ./matmul_par | extrair_tempo); echo "  Exec 1: ${t1}s"
t2=$(mpirun -np 4 --map-by node ./matmul_par | extrair_tempo); echo "  Exec 2: ${t2}s"
t3=$(mpirun -np 4 --map-by node ./matmul_par | extrair_tempo); echo "  Exec 3: ${t3}s"
T_4_2N=$(mediana $t1 $t2 $t3)
echo "  Mediana 4p/2nós: ${T_4_2N}s"

# ── Config 7: 8 processos, 2 nós (4 por nó) ──────
echo ""
echo ">>> CONFIG 7: 8 processos, 2 nós (4 por nó)"
t1=$(mpirun -np 8 --map-by node ./matmul_par | extrair_tempo); echo "  Exec 1: ${t1}s"
t2=$(mpirun -np 8 --map-by node ./matmul_par | extrair_tempo); echo "  Exec 2: ${t2}s"
t3=$(mpirun -np 8 --map-by node ./matmul_par | extrair_tempo); echo "  Exec 3: ${t3}s"
T_8_2N=$(mediana $t1 $t2 $t3)
echo "  Mediana 8p/2nós: ${T_8_2N}s"

echo ""
echo "=============================================="
echo " TABELA — FATOR 2 (comparação intra vs inter)"
echo "=============================================="
echo "Comparação com resultados do job_benchmark:"
echo ""
echo "Processos | Nós | Tp mediana (s) | Obs."
echo "----------|-----|----------------|-----"
echo "    4     |  1  | (ver benchmark)| intra-nó"
echo "    4     |  2  | ${T_4_2N}s      | inter-nós"
echo "    8     |  1  | (ver benchmark)| intra-nó"
echo "    8     |  2  | ${T_8_2N}s      | inter-nós"
echo ""
echo "Benchmark inter-nós concluído: $(date)"
