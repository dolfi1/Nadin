import difflib, pathlib
local = pathlib.Path('tools/remote_shot/capture_png.py').read_text(encoding='utf-8', errors='replace').splitlines()
remote = pathlib.Path('tmp_remote_capture_png.py').read_text(encoding='utf-8', errors='replace').splitlines()
print('local_lines',len(local),'remote_lines',len(remote))
for i,line in enumerate(difflib.unified_diff(remote, local, fromfile='remote', tofile='local', n=1)):
    print(line)
    if i>220:
        break
