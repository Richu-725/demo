@echo off
setlocal
if exist "%ProgramFiles%\Git\bin\bash.exe" (
    "%ProgramFiles%\Git\bin\bash.exe" %*
) else if exist "%ProgramFiles(x86)%\Git\bin\bash.exe" (
    "%ProgramFiles(x86)%\Git\bin\bash.exe" %*
) else (
    echo Git Bash not found. Install Git for Windows or update gbash.cmd.
    exit /b 1
)
