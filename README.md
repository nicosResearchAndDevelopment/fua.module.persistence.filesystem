# module.persistence.filesystem

## Interface

```ts
interface FileSystemStoreFactory extends DataStoreCoreFactory {
    store(graph: NamedNode, directory: string): FileSystemStore;
};
```