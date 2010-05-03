@echo off

set TEST_BIN=.\Stratosphere.Test\bin\Debug

pushd %TEST_BIN%
..\..\..\External\XUnit.NET\1.5\xunit.console.exe Stratosphere.Test.dll
popd
