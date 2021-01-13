# module.persistence.filesystem

- [Persistence](../module.persistence)

## Interface

```ts
interface FileSystemStoreFactory extends DataStoreCoreFactory {
    store(graph: NamedNode, directory: string): FileSystemStore;
};
```