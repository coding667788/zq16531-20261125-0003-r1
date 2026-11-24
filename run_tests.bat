@echo off
REM 运行全部 unittest（独立子进程崩溃/恢复测试 + 线性化对照）
cd /d "%~dp0"
python -m unittest discover -s tests -t . -v
