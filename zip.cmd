@echo off

if exist Stratosphere-1.0.0.0.zip del /F /Q Stratosphere-1.0.0.0.zip > nul

pushd .\Output
7z a -r ..\Stratosphere-1.0.0.0.zip * > nul
popd
