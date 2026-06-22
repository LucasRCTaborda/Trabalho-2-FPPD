package main

import (
	"fmt"
	"math/rand"
	"time"

	mpi "github.com/mnlphlp/gompi"
)

const SEED = 42

func main() {
	mpi.Init()
	defer mpi.Finalize()

	comm := mpi.NewComm(true)
	rank := comm.GetRank()
	size := comm.GetSize()

	N := 3000

	// Apenas o rank 0 imprime o cabeçalho
	if rank == 0 {
		fmt.Printf("Multiplicação de Matrizes Paralela com MPI — N=%d, Processos=%d\n", N, size)
		fmt.Printf("Seed: %d\n", SEED)
	}

	// -------------------------------------------------------
	// Todos os processos geram as matrizes A e B com a mesma seed.
	// Isso garante reprodutibilidade e evita broadcasts de matrizes
	// gigantescas. Cada processo só precisará das linhas que lhe
	// cabem de A, mas precisa de B inteira para o produto.
	// -------------------------------------------------------
	A := make([]float64, N*N)
	B := make([]float64, N*N)

	rng := rand.New(rand.NewSource(SEED))
	for i := 0; i < N*N; i++ {
		A[i] = rng.Float64()
		B[i] = rng.Float64()
	}

	// -------------------------------------------------------
	// Divisão de trabalho por linhas (row-based decomposition)
	// Cada processo calcula um bloco de linhas de C.
	// Se N não for divisível por size, o último processo recebe
	// as linhas restantes.
	// -------------------------------------------------------
	linhasPorProcesso := N / size
	linhaExtra := N % size

	// Calcular linha inicial e quantidade de linhas deste processo
	var startRow, numLinhas int
	if rank < linhaExtra {
		// Os primeiros 'linhaExtra' processos recebem uma linha a mais
		numLinhas = linhasPorProcesso + 1
		startRow = rank * numLinhas
	} else {
		numLinhas = linhasPorProcesso
		startRow = rank*numLinhas + linhaExtra
	}

	// Barreira antes de iniciar a medição
	comm.Barrier()

	var inicio time.Time
	if rank == 0 {
		inicio = time.Now()
		fmt.Printf("Iniciando computação paralela (startRow por processo varia)...\n")
	}

	// -------------------------------------------------------
	// Computação local: cada processo calcula suas linhas de C
	// C[i][j] = sum_{k=0}^{N-1} A[i][k] * B[k][j]
	// -------------------------------------------------------
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

	// -------------------------------------------------------
	// Coleta dos resultados no rank 0
	// Modelo: Mestre-Escravo com Send/Recv
	// -------------------------------------------------------

	if rank == 0 {
		// Rank 0 monta a matriz C completa
		C := make([]float64, N*N)

		// Copiar as linhas calculadas pelo próprio rank 0
		copy(C[startRow*N:(startRow+numLinhas)*N], localC)

		// Receber as linhas dos demais processos
		for src := 1; src < size; src++ {
			// Calcular quantas linhas este processo enviou e a partir de onde
			var srcStart, srcLinhas int
			if src < linhaExtra {
				srcLinhas = linhasPorProcesso + 1
				srcStart = src * srcLinhas
			} else {
				srcLinhas = linhasPorProcesso
				srcStart = src*srcLinhas + linhaExtra
			}

			buf := make([]float64, srcLinhas*N)
			comm.Recv(buf, src, 0)
			copy(C[srcStart*N:(srcStart+srcLinhas)*N], buf)
		}

		tempo := time.Since(inicio)

		fmt.Printf("Tempo de execução paralela: %.4f segundos\n", tempo.Seconds())

		// Valores de verificação: cantos da matriz C
		fmt.Printf("\n=== Verificação (cantos de C) ===\n")
		fmt.Printf("C[0][0]       = %.6f\n", C[0])
		fmt.Printf("C[0][N-1]     = %.6f\n", C[N-1])
		fmt.Printf("C[N-1][0]     = %.6f\n", C[(N-1)*N])
		fmt.Printf("C[N-1][N-1]   = %.6f\n", C[(N-1)*N+(N-1)])

		// Checksum
		var checksum float64
		for i := 0; i < N*N; i++ {
			checksum += C[i]
		}
		fmt.Printf("Checksum total = %.6f\n", checksum)

	} else {
		// Processos trabalhadores enviam suas linhas ao rank 0
		comm.Send(localC, 0, 0)
	}
}
