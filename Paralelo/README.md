BENCHMARK - Multiplicação de Matrizes N=3000
 Data: Mon Jun 22 15:35:58 -03 2026
 Nó: atlantica01
==============================================

[1/2] Compilando versão sequencial...
  OK
[2/2] Compilando versão paralela...
go: inconsistent vendoring in /home/fppd3212/Trabalho-2-FPPD/Paralelo:
        github.com/mnlphlp/gompi@v0.4.0: is explicitly required in go.mod, but not marked as explicit in vendor/modules.txt

        To ignore the vendor directory, use -mod=readonly or -mod=mod.
        To sync the vendor directory, run:
                go mod vendor
  ERRO na compilação paralela
/var/spool/slurm/d/job34667/slurm_script: line 8: module: command not found
/var/spool/slurm/d/job34667/slurm_script: line 9: module: command not found
==============================================
 BENCHMARK - Multiplicação de Matrizes N=3000
 Data: Mon Jun 22 15:41:59 -03 2026
 Nó: atlantica05
==============================================

[1/2] Compilando versão sequencial...
  OK
[2/2] Compilando versão paralela...
go: inconsistent vendoring in /home/fppd3212/Trabalho-2-FPPD/Paralelo:
        github.com/mnlphlp/gompi@v0.4.0: is explicitly required in go.mod, but not marked as explicit in vendor/modules.txt

        To ignore the vendor directory, use -mod=readonly or -mod=mod.
        To sync the vendor directory, run:
                go mod vendor
  ERRO na compilação paralela
/var/spool/slurm/d/job34668/slurm_script: line 8: module: command not found
/var/spool/slurm/d/job34668/slurm_script: line 9: module: command not found
==============================================
 BENCHMARK - Multiplicação de Matrizes N=3000
 Data: Mon Jun 22 15:50:13 -03 2026
 Nó: atlantica05
==============================================

[1/2] Compilando versão sequencial...
  OK
[2/2] Compilando versão paralela...
# github.com/mnlphlp/gompi/wrap
# [pkg-config --cflags  -- ompi ompi]
Package ompi was not found in the pkg-config search path.
Perhaps you should add the directory containing `ompi.pc'
to the PKG_CONFIG_PATH environment variable
No package 'ompi' found
Package ompi was not found in the pkg-config search path.
Perhaps you should add the directory containing `ompi.pc'
to the PKG_CONFIG_PATH environment variable
No package 'ompi' found
  ERRO na compilação paralela
/var/spool/slurm/d/job34669/slurm_script: line 8: module: command not found
/var/spool/slurm/d/job34669/slurm_script: line 9: module: command not found
==============================================
 BENCHMARK - Multiplicação de Matrizes N=3000
 Data: Mon Jun 22 16:09:16 -03 2026
 Nó: atlantica01
==============================================

[1/2] Compilando versão sequencial...
  OK
[2/2] Compilando versão paralela...
  OK

==============================================
 EXECUTANDO TESTES (3x cada config)
==============================================

>>> CONFIG 1: Sequencial (1 processo)
  Execução 1/3...
    Tempo: 58.5334s
  Execução 2/3...
    Tempo: 58.4252s
  Execução 3/3...
    Tempo: 58.4752s
  Mediana sequencial: 58.4752s

>>> CONFIG: Paralelo com 2 processos
  Execução 1/3...
    Tempo: 30.3831s
  Execução 2/3...
    Tempo: 30.4031s
  Execução 3/3...
    Tempo: 30.2317s
  Mediana: 30.3831s  |  Speedup: 1.924x  |  Eficiência: 90.0%

>>> CONFIG: Paralelo com 4 processos
  Execução 1/3...
    Tempo: 15.5355s
  Execução 2/3...
    Tempo: 15.5107s
  Execução 3/3...
    Tempo: 15.3565s
  Mediana: 15.5107s  |  Speedup: 3.769x  |  Eficiência: 90.0%

>>> CONFIG: Paralelo com 8 processos
  Execução 1/3...
    Tempo: 8.9410s
  Execução 2/3...
    Tempo: 8.9217s
  Execução 3/3...
    Tempo: 8.9421s
  Mediana: 8.9410s  |  Speedup: 6.540x  |  Eficiência: 80.0%

>>> CONFIG: Paralelo com 16 processos
  Execução 1/3...
    Tempo: 8.6308s
  Execução 2/3...
    Tempo: 8.6386s
  Execução 3/3...
    Tempo: 8.6456s
  Mediana: 8.6386s  |  Speedup: 6.769x  |  Eficiência: 40.0%

Benchmark concluído: Mon Jun 22 16:15:30 -03 2026