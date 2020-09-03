# fua.module.persistence.filesystem

-[Microsoft, de](https://www.windows-faq.de/2016/11/06/windows-10-ordnernamen-und-dateinamen-groesser-260-zeichen/)

```
HKEY_LOCAL_MACHINE\SYSTEM\CurrentControlSet\Control\FileSystem
```

## Recommendation

Put graph-folder to root, to keep length of pathes as shor as possible.

### Windows

```
C:/graph/ -.
           |
           - hashed_URI.ttl
           - aasdfsafasfkljhqweruztzyxcvlksjhdflkasjhflkashjflkasjhflkasfhlaksjfh.ttl
           - ...
```
 

---