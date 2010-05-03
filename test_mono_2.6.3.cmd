@echo off

set TEST_BIN=.\Stratosphere.Test\bin\Debug
set MONO_HOME=C:\Program Files\Mono-2.6.3

pushd %TEST_BIN%
"%MONO_HOME%\bin\mono.exe" ..\..\..\External\XUnit.NET\1.5\xunit.console.exe Stratosphere.Test.dll
popd
