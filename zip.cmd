@echo off

if exist Stratosphere-1.0.0.3.zip del /F /Q Stratosphere-1.0.0.3.zip > nul

pushd .\Output
7z a -r ..\Stratosphere-1.0.0.3.zip * > nul
popd
