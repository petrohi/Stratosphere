@echo off

setlocal enabledelayedexpansion
set version=
for /f "delims=" %%l in (Version.txt) do (set version=%%l)

if exist Stratosphere-%version%.zip del /F /Q Stratosphere-%version%.zip > nul

pushd .\Output
7z a -r ..\Stratosphere-%version%.zip * > nul
popd
