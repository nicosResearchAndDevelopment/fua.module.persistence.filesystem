# @fua/module.persistence.filesystem

- [Persistence](https://git02.int.nsc.ag/Research/fua/lib/module.persistence)

## Interface

```ts
interface FileSystemStoreFactory extends DataStoreCoreFactory {
    store(graph: NamedNode, directory: string): FileSystemStore;
};
```
