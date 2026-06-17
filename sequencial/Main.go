package main

import (
	"fmt"
	"math/rand"
	"time"
)

const SEED = 42

func main() {
	N := 3000

	fmt.Printf("Multiplicação de Matrizes Sequencial — N=%d\n", N)
	fmt.Printf("Seed: %d\n", SEED)

	// Alocar matrizes como slices 1D (row-major) para facilitar envio via MPI na versão paralela
	A := make([]float64, N*N)
	B := make([]float64, N*N)
	C := make([]float64, N*N)

	// Inicializar com valores aleatórios usando seed fixa
	rng := rand.New(rand.NewSource(SEED))
	for i := 0; i < N*N; i++ {
		A[i] = rng.Float64()
		B[i] = rng.Float64()
	}

	fmt.Println("Matrizes geradas. Iniciando multiplicação sequencial...")

	// Medir apenas o tempo de computação (excluindo geração das matrizes)
	inicio := time.Now()

	// Algoritmo ingênuo: triplo loop (i, j, k)
	// C[i][j] = sum_{k=0}^{N-1} A[i][k] * B[k][j]
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

	// Valores de verificação: cantos da matriz C
	fmt.Printf("\n=== Verificação (cantos de C) ===\n")
	fmt.Printf("C[0][0]       = %.6f\n", C[0])
	fmt.Printf("C[0][N-1]     = %.6f\n", C[N-1])
	fmt.Printf("C[N-1][0]     = %.6f\n", C[(N-1)*N])
	fmt.Printf("C[N-1][N-1]   = %.6f\n", C[(N-1)*N+(N-1)])

	// Checksum simples para validação
	var checksum float64
	for i := 0; i < N*N; i++ {
		checksum += C[i]
	}
	fmt.Printf("Checksum total = %.6f\n", checksum)
}
