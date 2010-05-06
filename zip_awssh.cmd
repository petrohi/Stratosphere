@echo off

if exist Stratosphere-AwsSh-1.0.0.3.zip del /F /Q Stratosphere-AwsSh-1.0.0.3.zip > nul

pushd .\Output.AwsSh
7z a -r ..\Stratosphere-AwsSh-1.0.0.3.zip * > nul
popd
