## Notes

```sql
select MAX(octet_length(mol_to_pkl(rdkit_mol))), MIN(octet_length(mol_to_pkl(rdkit_mol))), AVG(octet_length(mol_to_pkl(rdkit_mol))) from compound_structures;

```

```shell
psql postgresql://postgres:example@localhost:5435
```

```shell
\connect chembl_33;
```
