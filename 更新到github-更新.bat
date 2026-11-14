@echo off
SETLOCAL EnableDelayedExpansion

:: 1. 配置业务变量
SET "UNC_PATH=\\192.168.69.11\root\root\causal_plugin"
SET "PROXY_URL=http://192.168.66.24:65532"

echo [INFO] 正在进入工作目录...
pushd "%UNC_PATH%" || (
    echo [ERROR] 无法映射路径，请检查网络连接。
    pause
    exit /b
)

:: 3. 仓库检查
if not exist ".git" (
    echo [ERROR] 找不到 .git 目录，请确认路径: %CD%
    popd
    pause
    exit /b
)

:: 4. 处理时间字符串（去除空格，防止文件名/参数非法）
set "cur_date=%date%"
set "cur_time=%time: =0%"
set "commit_msg=Auto-update %cur_date% %cur_time%"

echo [INFO] 提交信息: "%commit_msg%"

echo [INFO] 正在同步...
git add .
git commit -m "%commit_msg%"

:: 推送逻辑
echo [INFO] 正在推送至 GitHub...
git -c http.proxy=%PROXY_URL% -c https.proxy=%PROXY_URL% push origin main

if !errorlevel! eq 0 (
    echo.
    echo [SUCCESS] 任务圆满完成！
) else (
    echo.
    echo [ERROR] 推送过程中出现问题。
)

:: 5. 清理并强制暂停
popd
echo [INFO] 脚本运行完毕，按任意键关闭窗口。
pause