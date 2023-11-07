# -*- mode: python ; coding: utf-8 -*-


block_cipher = None


a = Analysis(
    ['gz_operator.py'],
    pathex=[],
    binaries=[],
    datas=[('config', 'config'), ('databases', 'databases'), ('logic_subject', 'logic_subject'), ('template_file', 'template_file'),
    ('my_scripts', 'my_scripts'), ('task', 'task')],
    hiddenimports=['apscheduler'],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)


# 将需要排除的模块写到一个列表（不带 .py）
my_modules = ['main']
# 将被排除的模块添加到 a.datas
for name in my_modules:
    source_file = name + '.py'
    dest_file = name + '.py'
    a.datas.append((source_file, dest_file, 'DATA'))
# 筛选 a.pure
a.pure = [x for x in a.pure if x[0] not in my_modules]


pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name='gz_operator',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=True,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)
coll = COLLECT(
    exe,
    a.binaries,
    a.zipfiles,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name='gz_operator',
)
