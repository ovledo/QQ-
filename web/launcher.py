# -*- coding: utf-8 -*-
"""轻量启动器 - 编译为exe后直接调用 python main.py，始终使用最新源码"""
import subprocess
import sys
import os
import shutil
from pathlib import Path
import time
import socket
import webbrowser
from urllib import request as _url_request

def _msgbox(msg, title="QQ三国资产管理 - 错误"):
    import ctypes
    ctypes.windll.user32.MessageBoxW(0, msg, title, 0x10)

def _log(launcher_dir, msg):
    """写日志到 launcher.log 方便排错"""
    try:
        log_path = os.path.join(launcher_dir, "launcher.log")
        with open(log_path, "a", encoding="utf-8") as f:
            from datetime import datetime
            f.write(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}\n")
    except Exception:
        pass

_launcher_mutex_handle = None
_ALLOW_PACKAGED_FALLBACK_ENV = 'QQSG_ALLOW_PACKAGED_FALLBACK'

def _is_truthy(value):
    return (value or '').strip().lower() in ('1', 'true', 'yes', 'on')

def _allow_packaged_fallback():
    return _is_truthy(os.environ.get(_ALLOW_PACKAGED_FALLBACK_ENV))

def _sync_packaged_static_dir(launcher_dir, packaged_target):
    if not launcher_dir or not packaged_target:
        return
    src_dir = Path(launcher_dir) / 'static'
    dst_dir = Path(os.path.dirname(os.path.abspath(packaged_target))) / '_internal' / 'static'
    if not src_dir.is_dir() or not dst_dir.is_dir():
        return
    try:
        changed = False
        for src_path in src_dir.rglob('*'):
            rel_path = src_path.relative_to(src_dir)
            dst_path = dst_dir / rel_path
            if src_path.is_dir():
                dst_path.mkdir(parents=True, exist_ok=True)
                continue
            dst_path.parent.mkdir(parents=True, exist_ok=True)
            if dst_path.exists() and src_path.stat().st_size == dst_path.stat().st_size and src_path.read_bytes() == dst_path.read_bytes():
                continue
            shutil.copyfile(src_path, dst_path)
            changed = True
        if changed:
            _log(launcher_dir, f"已同步源码静态资源到目录版: {dst_dir}")
    except Exception as e:
        _log(launcher_dir, f"同步目录版静态资源失败: {e}")

def _acquire_single_instance():
    global _launcher_mutex_handle
    if os.name != 'nt':
        return True
    try:
        import ctypes
        kernel32 = ctypes.windll.kernel32
        handle = kernel32.CreateMutexW(None, False, "Local\\QQSGLauncherSingleton")
        if not handle:
            return True
        if kernel32.GetLastError() == 183:
            kernel32.CloseHandle(handle)
            return False
        _launcher_mutex_handle = handle
    except Exception:
        return True
    return True

def _port_in_use(port):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(0.5)
        return s.connect_ex(("127.0.0.1", port)) == 0

def _health_ok(port):
    try:
        with _url_request.urlopen(f"http://127.0.0.1:{port}/api/health", timeout=1.5) as resp:
            return getattr(resp, 'status', 200) == 200
    except Exception:
        return False

def _find_existing_service(start_port=8000, max_tries=20):
    for candidate in range(start_port, start_port + max_tries):
        if _health_ok(candidate):
            return candidate
    return None

def _wait_for_starting_service(start_port=8000, max_tries=20, wait_seconds=6):
    if not _port_in_use(start_port):
        return None
    deadline = time.time() + wait_seconds
    while time.time() < deadline:
        existing_port = _find_existing_service(start_port, max_tries)
        if existing_port is not None:
            return existing_port
        if not _port_in_use(start_port):
            return None
        time.sleep(0.5)
    return _find_existing_service(start_port, max_tries)

def _resolve_launcher_dir():
    if not getattr(sys, 'frozen', False):
        return os.path.dirname(os.path.abspath(__file__))
    exe_dir = Path(sys.executable).resolve().parent
    candidates = [
        exe_dir / "web",
        exe_dir,
        exe_dir.parent / "web",
        exe_dir.parent.parent,
        exe_dir.parent.parent / "web",
        exe_dir / "QQ三国" / "web",
        exe_dir.parent / "QQ三国" / "web",
        exe_dir.parent.parent / "QQ三国" / "web",
    ]
    seen = set()
    for candidate in candidates:
        candidate = os.path.abspath(str(candidate))
        if candidate in seen:
            continue
        seen.add(candidate)
        if os.path.exists(os.path.join(candidate, "main.py")):
            return candidate
    return os.path.abspath(str(candidates[0]))

def _resolve_packaged_target():
    base_dir = os.path.dirname(os.path.abspath(sys.executable if getattr(sys, 'frozen', False) else __file__))
    current_exe = os.path.abspath(sys.executable) if getattr(sys, 'frozen', False) else ""
    candidates = [
        os.path.join(base_dir, "dist", "QQ三国资产管理", "QQ三国资产管理.exe"),
        os.path.join(base_dir, "web", "dist", "QQ三国资产管理", "QQ三国资产管理.exe"),
        os.path.join(base_dir, "QQ三国", "web", "dist", "QQ三国资产管理", "QQ三国资产管理.exe"),
        os.path.join(os.path.dirname(base_dir), "web", "dist", "QQ三国资产管理", "QQ三国资产管理.exe"),
        os.path.join(os.path.dirname(base_dir), "QQ三国", "web", "dist", "QQ三国资产管理", "QQ三国资产管理.exe"),
    ]
    for candidate in candidates:
        candidate = os.path.abspath(candidate)
        if current_exe and candidate == current_exe:
            continue
        if os.path.exists(candidate):
            return candidate
    return None

def _resolve_db_dir(launcher_dir, packaged_target):
    override = (os.environ.get('QQSG_DB_DIR') or '').strip()
    if override:
        return override
    candidates = []
    if launcher_dir:
        candidates.extend([
            launcher_dir,
            os.path.dirname(launcher_dir),
        ])
    if packaged_target:
        target_dir = os.path.dirname(os.path.abspath(packaged_target))
        candidates.extend([
            target_dir,
            os.path.dirname(target_dir),
            os.path.dirname(os.path.dirname(target_dir)),
        ])
    appdata_dir = os.path.join(os.environ.get('LOCALAPPDATA', os.path.expanduser('~')), 'QQ三国资产管理')
    candidates.append(appdata_dir)
    seen = set()
    for candidate in candidates:
        if not candidate:
            continue
        candidate = os.path.abspath(candidate)
        if candidate in seen:
            continue
        seen.add(candidate)
        if os.path.exists(os.path.join(candidate, 'qq_sanguo.db')):
            return candidate
    if launcher_dir:
        return launcher_dir
    if packaged_target:
        return os.path.dirname(os.path.abspath(packaged_target))
    return appdata_dir

def main():
    launcher_dir = _resolve_launcher_dir()
    packaged_target = _resolve_packaged_target()
    main_py = os.path.join(launcher_dir, "main.py")
    source_available = os.path.exists(main_py)
    allow_packaged_fallback = _allow_packaged_fallback()

    if packaged_target:
        _sync_packaged_static_dir(launcher_dir, packaged_target)

    if not _acquire_single_instance():
        _log(launcher_dir, "检测到启动器已有实例，等待现有服务就绪")
        existing_port = _wait_for_starting_service(8000, 20, 8)
        if existing_port is not None:
            _log(launcher_dir, f"复用正在启动的服务: http://127.0.0.1:{existing_port}")
            webbrowser.open(f"http://127.0.0.1:{existing_port}")
            return
        _msgbox("程序正在启动，请稍候再试")
        return

    db_dir = _resolve_db_dir(launcher_dir, packaged_target)
    _log(launcher_dir, f"启动器开始, packaged_target={packaged_target}, main.py={main_py}, db_dir={db_dir}, source_available={source_available}, allow_packaged_fallback={allow_packaged_fallback}")

    if not packaged_target and not source_available:
        _msgbox(f"找不到 main.py\n路径: {main_py}")
        sys.exit(1)

    existing_port = _find_existing_service(8000, 20)
    if existing_port is not None:
        _log(launcher_dir, f"检测到已有服务运行: http://127.0.0.1:{existing_port}")
        webbrowser.open(f"http://127.0.0.1:{existing_port}")
        return
    starting_port = _wait_for_starting_service(8000, 20, 6)
    if starting_port is not None:
        _log(launcher_dir, f"检测到服务正在启动，复用: http://127.0.0.1:{starting_port}")
        webbrowser.open(f"http://127.0.0.1:{starting_port}")
        return

    python_paths = [
        sys.executable if (sys.executable and os.path.basename(sys.executable).lower().startswith('python')) else None,
        r"C:\Users\93281\AppData\Local\Python\pythoncore-3.14-64\python.exe",
        "python",
        "python3",
    ]
    python_exe = None
    for p in [p for p in python_paths if p]:
        try:
            r = subprocess.run([p, "--version"], capture_output=True, timeout=5,
                               creationflags=0x08000000)
            if r.returncode == 0:
                python_exe = p
                ver = (r.stdout or r.stderr).decode(errors="replace").strip()
                _log(launcher_dir, f"Python: {p} -> {ver}")
                break
        except Exception:
            continue

    CREATE_NO_WINDOW = 0x08000000
    launch_env = os.environ.copy()
    launch_env['QQSG_DB_DIR'] = db_dir
    launch_env['QQSG_SOURCE_WEB_DIR'] = launcher_dir

    if python_exe and source_available:
        server_log_path = os.path.join(launcher_dir, "launcher-server.log")
        _log(launcher_dir, f"启动源码版: {python_exe} {main_py}")
        with open(server_log_path, "a", encoding="utf-8", errors="replace") as log_fp:
            proc = subprocess.Popen(
                [python_exe, main_py],
                cwd=launcher_dir,
                creationflags=CREATE_NO_WINDOW,
                stdout=log_fp,
                stderr=log_fp,
                env=launch_env,
            )

            time.sleep(4)
            rc = proc.poll()
        if rc is None:
            _log(launcher_dir, f"源码版启动成功, PID={proc.pid}")
            return
        existing_port = _find_existing_service(8000, 20)
        if rc == 0 and existing_port is not None:
            _log(launcher_dir, f"源码版已复用已有服务: http://127.0.0.1:{existing_port}")
            webbrowser.open(f"http://127.0.0.1:{existing_port}")
            return
        stderr = ""
        try:
            with open(server_log_path, "r", encoding="utf-8", errors="replace") as f:
                stderr = f.read()[-1500:]
        except Exception:
            pass
        _log(launcher_dir, f"源码版启动失败! 退出码={rc}\n{stderr}")
        if not allow_packaged_fallback:
            _msgbox(f"源码版启动失败，已阻止回退到旧目录版。\n\n请查看: {server_log_path}")
            sys.exit(1)

    if source_available and not python_exe and not allow_packaged_fallback:
        _msgbox("检测到源码存在，但未找到可用 Python。当前已禁止自动回退到目录版。")
        sys.exit(1)

    if packaged_target:
        _log(launcher_dir, f"回退启动目录版: {packaged_target}")
        proc = subprocess.Popen(
            [packaged_target],
            cwd=os.path.dirname(packaged_target),
            creationflags=CREATE_NO_WINDOW,
            env=launch_env,
        )
        time.sleep(4)
        rc = proc.poll()
        if rc is not None:
            existing_port = _find_existing_service(8000, 20)
            if rc == 0 and existing_port is not None:
                _log(launcher_dir, f"目录版已复用已有服务: http://127.0.0.1:{existing_port}")
                webbrowser.open(f"http://127.0.0.1:{existing_port}")
                return
            _log(launcher_dir, f"目录版启动失败! 退出码={rc}, target={packaged_target}")
            _msgbox(f"桌面程序启动失败 (退出码 {rc})\n\n目标: {packaged_target}")
            sys.exit(1)
        _log(launcher_dir, f"目录版启动成功, PID={proc.pid}")
        return

    _msgbox("找不到 Python 解释器，请确认已安装 Python")
    sys.exit(1)

if __name__ == "__main__":
    main()
