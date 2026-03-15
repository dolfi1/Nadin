import difflib, pathlib
s=pathlib.Path('src/native_app.py').read_text(encoding='utf-8',errors='replace').splitlines()
t=pathlib.Path('tmp_head_native_app.py').read_text(encoding='utf-8',errors='replace').splitlines()
for i,line in enumerate(difflib.unified_diff(s,t,fromfile='src',tofile='tmp',n=1)):
    print(line)
    if i>300:
        break
