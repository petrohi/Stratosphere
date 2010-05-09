@echo off

setlocal enabledelayedexpansion
set version=
for /f "delims=" %%l in (Version.txt) do (set version=%%l)

if exist Stratosphere-AwsSh-%version%.zip del /F /Q Stratosphere-AwsSh-%version%.zip > nul

pushd .\Output.AwsSh
7z a -r ..\Stratosphere-AwsSh-%version%.zip * > nul
popd
