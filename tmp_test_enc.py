orig='\u0421\u043a\u0440\u0438\u043d\u0448\u043e\u0442'
moj=orig.encode('utf-8').decode('cp1251')
print([hex(ord(ch)) for ch in moj])
rev=moj.encode('cp1251').decode('utf-8')
print([hex(ord(ch)) for ch in rev])
