use anyhow::{anyhow, Context, Result};
use arrow::array::{
    Array, ArrowPrimitiveType, BinaryArray, BooleanArray, BooleanBuilder, GenericByteArray,
    Int16Array, Int32Array, Int64Array, Int8Array, PrimitiveArray, StringArray, UInt16Array,
    UInt32Array, UInt64Array, UInt8Array,
};
use arrow::buffer::BooleanBuffer;
use arrow::compute;
use arrow::datatypes::{ByteArrayType, DataType, ToByteSlice};
use arrow::record_batch::RecordBatch;
use arrow::row::{RowConverter, SortField};
use hashbrown::HashTable;
use rayon::prelude::*;
use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::sync::Arc;
use xxhash_rust::xxh3::xxh3_64;

type TableName = String;
type FieldName = String;

#[derive(Clone)]
pub struct Query {
    pub selection: Arc<BTreeMap<TableName, Vec<TableSelection>>>,
    pub fields: BTreeMap<TableName, Vec<FieldName>>,
}

impl Query {
    pub fn add_request_and_include_fields(&mut self) -> Result<()> {
        for (table_name, selections) in self.selection.iter() {
            for selection in selections.iter() {
                for col_name in selection.filters.keys() {
                    let table_fields = self
                        .fields
                        .get_mut(table_name)
                        .with_context(|| format!("get fields for table {}", table_name))?;
                    table_fields.push(col_name.to_owned());
                }

                for include in selection.include.iter() {
                    let other_table_fields = self
                        .fields
                        .get_mut(&include.other_table_name)
                        .with_context(|| {
                            format!("get fields for other table {}", include.other_table_name)
                        })?;
                    other_table_fields.extend_from_slice(&include.other_table_field_names);
                    let table_fields = self
                        .fields
                        .get_mut(table_name)
                        .with_context(|| format!("get fields for table {}", table_name))?;
                    table_fields.extend_from_slice(&include.field_names);
                }
            }
        }

        Ok(())
    }
}

pub struct TableSelection {
    pub filters: BTreeMap<FieldName, Filter>,
    pub include: Vec<Include>,
}

pub struct Include {
    pub other_table_name: TableName,
    pub field_names: Vec<FieldName>,
    pub other_table_field_names: Vec<FieldName>,
}

pub enum Filter {
    Contains(Contains),
    StartsWith(StartsWith),
    Bool(bool),
}

impl Filter {
    pub fn contains(arr: Arc<dyn Array>) -> Result<Self> {
        Ok(Self::Contains(Contains::new(arr)?))
    }

    pub fn starts_with(arr: Arc<dyn Array>) -> Result<Self> {
        Ok(Self::StartsWith(StartsWith::new(arr)?))
    }

    pub fn bool(b: bool) -> Self {
        Self::Bool(b)
    }

    fn check(&self, arr: &dyn Array) -> Result<BooleanArray> {
        match self {
            Self::Contains(ct) => ct.contains(arr),
            Self::StartsWith(sw) => sw.starts_with(arr),
            Self::Bool(b) => {
                let arr = arr
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .context("cast array to boolean array")?;

                let mut filter = if *b {
                    arr.clone()
                } else {
                    compute::not(arr).context("negate array")?
                };

                if let Some(nulls) = filter.nulls() {
                    if nulls.null_count() > 0 {
                        let nulls = BooleanArray::from(nulls.inner().clone());
                        filter = compute::and(&filter, &nulls).unwrap();
                    }
                }

                Ok(filter)
            }
        }
    }
}

pub struct Contains {
    array: Arc<dyn Array>,
    hash_table: Option<HashTable<usize>>,
}

impl Contains {
    fn ht_from_primitive<T: ArrowPrimitiveType>(arr: &PrimitiveArray<T>) -> HashTable<usize> {
        assert!(!arr.is_nullable());

        let mut ht = HashTable::with_capacity(arr.len());

        for (i, v) in arr.values().iter().enumerate() {
            ht.insert_unique(xxh3_64(v.to_byte_slice()), i, |i| {
                xxh3_64(unsafe { arr.value_unchecked(*i).to_byte_slice() })
            });
        }

        ht
    }

    fn ht_from_bytes<T: ByteArrayType<Offset = i32>>(
        arr: &GenericByteArray<T>,
    ) -> HashTable<usize> {
        assert!(!arr.is_nullable());

        let mut ht = HashTable::with_capacity(arr.len());

        for (i, v) in iter_byte_array_without_validity(arr).enumerate() {
            ht.insert_unique(xxh3_64(v), i, |i| {
                xxh3_64(unsafe { byte_array_get_unchecked(arr, *i) })
            });
        }

        ht
    }

    fn ht_from_array(array: &dyn Array) -> Result<HashTable<usize>> {
        let ht = match *array.data_type() {
            DataType::UInt8 => {
                let array = array.as_any().downcast_ref::<UInt8Array>().unwrap();
                Self::ht_from_primitive(array)
            }
            DataType::UInt16 => {
                let array = array.as_any().downcast_ref::<UInt16Array>().unwrap();
                Self::ht_from_primitive(array)
            }
            DataType::UInt32 => {
                let array = array.as_any().downcast_ref::<UInt32Array>().unwrap();
                Self::ht_from_primitive(array)
            }
            DataType::UInt64 => {
                let array = array.as_any().downcast_ref::<UInt64Array>().unwrap();
                Self::ht_from_primitive(array)
            }
            DataType::Int8 => {
                let array = array.as_any().downcast_ref::<Int8Array>().unwrap();
                Self::ht_from_primitive(array)
            }
            DataType::Int16 => {
                let array = array.as_any().downcast_ref::<Int16Array>().unwrap();
                Self::ht_from_primitive(array)
            }
            DataType::Int32 => {
                let array = array.as_any().downcast_ref::<Int32Array>().unwrap();
                Self::ht_from_primitive(array)
            }
            DataType::Int64 => {
                let array = array.as_any().downcast_ref::<Int64Array>().unwrap();
                Self::ht_from_primitive(array)
            }
            DataType::Binary => {
                let array = array.as_any().downcast_ref::<BinaryArray>().unwrap();
                Self::ht_from_bytes(array)
            }
            DataType::Utf8 => {
                let array = array.as_any().downcast_ref::<StringArray>().unwrap();
                Self::ht_from_bytes(array)
            }
            _ => {
                return Err(anyhow!("unsupported data type: {}", array.data_type()));
            }
        };

        Ok(ht)
    }

    pub fn new(array: Arc<dyn Array>) -> Result<Self> {
        if array.is_nullable() {
            return Err(anyhow!(
                "cannot construct contains filter with a nullable array"
            ));
        }

        // only use a hash table if there are more than 128 elements
        let hash_table = if array.len() >= 128 {
            Some(Self::ht_from_array(&array).context("construct hash table")?)
        } else {
            None
        };

        Ok(Self { hash_table, array })
    }

    fn contains(&self, arr: &dyn Array) -> Result<BooleanArray> {
        if arr.data_type() != self.array.data_type() {
            return Err(anyhow!(
                "filter array is of type {} but array to be filtered is of type {}",
                self.array.data_type(),
                arr.data_type(),
            ));
        }
        assert!(!self.array.is_nullable());

        let filter = match *arr.data_type() {
            DataType::UInt8 => {
                let self_arr = self.array.as_any().downcast_ref::<UInt8Array>().unwrap();
                let other_arr = arr.as_any().downcast_ref().unwrap();
                self.contains_primitive(self_arr, other_arr)
            }
            DataType::UInt16 => {
                let self_arr = self.array.as_any().downcast_ref::<UInt16Array>().unwrap();
                let other_arr = arr.as_any().downcast_ref().unwrap();
                self.contains_primitive(self_arr, other_arr)
            }
            DataType::UInt32 => {
                let self_arr = self.array.as_any().downcast_ref::<UInt32Array>().unwrap();
                let other_arr = arr.as_any().downcast_ref().unwrap();
                self.contains_primitive(self_arr, other_arr)
            }
            DataType::UInt64 => {
                let self_arr = self.array.as_any().downcast_ref::<UInt64Array>().unwrap();
                let other_arr = arr.as_any().downcast_ref().unwrap();
                self.contains_primitive(self_arr, other_arr)
            }
            DataType::Int8 => {
                let self_arr = self.array.as_any().downcast_ref::<Int8Array>().unwrap();
                let other_arr = arr.as_any().downcast_ref().unwrap();
                self.contains_primitive(self_arr, other_arr)
            }
            DataType::Int16 => {
                let self_arr = self.array.as_any().downcast_ref::<Int16Array>().unwrap();
                let other_arr = arr.as_any().downcast_ref().unwrap();
                self.contains_primitive(self_arr, other_arr)
            }
            DataType::Int32 => {
                let self_arr = self.array.as_any().downcast_ref::<Int32Array>().unwrap();
                let other_arr = arr.as_any().downcast_ref().unwrap();
                self.contains_primitive(self_arr, other_arr)
            }
            DataType::Int64 => {
                let self_arr = self.array.as_any().downcast_ref::<Int64Array>().unwrap();
                let other_arr = arr.as_any().downcast_ref().unwrap();
                self.contains_primitive(self_arr, other_arr)
            }
            DataType::Binary => {
                let self_arr = self.array.as_any().downcast_ref::<BinaryArray>().unwrap();
                let other_arr = arr.as_any().downcast_ref().unwrap();
                self.contains_bytes(self_arr, other_arr)
            }
            DataType::Utf8 => {
                let self_arr = self.array.as_any().downcast_ref::<StringArray>().unwrap();
                let other_arr = arr.as_any().downcast_ref().unwrap();
                self.contains_bytes(self_arr, other_arr)
            }
            _ => {
                return Err(anyhow!("unsupported data type: {}", arr.data_type()));
            }
        };

        let mut filter = filter;

        if let Some(nulls) = arr.nulls() {
            if nulls.null_count() > 0 {
                let nulls = BooleanArray::from(nulls.inner().clone());
                filter = compute::and(&filter, &nulls).unwrap();
            }
        }

        Ok(filter)
    }

    fn contains_primitive<T: ArrowPrimitiveType>(
        &self,
        self_arr: &PrimitiveArray<T>,
        other_arr: &PrimitiveArray<T>,
    ) -> BooleanArray {
        let mut filter = BooleanBuilder::with_capacity(other_arr.len());

        if let Some(ht) = self.hash_table.as_ref() {
            let hash_one = |v: &T::Native| -> u64 { xxh3_64(v.to_byte_slice()) };

            for v in other_arr.values().iter() {
                let c = ht
                    .find(hash_one(v), |idx| unsafe {
                        self_arr.values().get_unchecked(*idx) == v
                    })
                    .is_some();
                filter.append_value(c);
            }
        } else {
            for v in other_arr.values().iter() {
                filter.append_value(self_arr.values().iter().any(|x| x == v));
            }
        }

        filter.finish()
    }

    fn contains_bytes<T: ByteArrayType<Offset = i32>>(
        &self,
        self_arr: &GenericByteArray<T>,
        other_arr: &GenericByteArray<T>,
    ) -> BooleanArray {
        let mut filter = BooleanBuilder::with_capacity(other_arr.len());

        if let Some(ht) = self.hash_table.as_ref() {
            for v in iter_byte_array_without_validity(other_arr) {
                let c = ht
                    .find(xxh3_64(v), |idx| unsafe {
                        byte_array_get_unchecked(self_arr, *idx) == v
                    })
                    .is_some();
                filter.append_value(c);
            }
        } else {
            for v in iter_byte_array_without_validity(other_arr) {
                filter.append_value(iter_byte_array_without_validity(self_arr).any(|x| x == v));
            }
        }

        filter.finish()
    }
}

pub struct StartsWith {
    array: Arc<dyn Array>,
}

impl StartsWith {
    pub fn new(array: Arc<dyn Array>) -> Result<Self> {
        if array.is_nullable() {
            return Err(anyhow!(
                "cannot construct starts_with filter with a nullable array"
            ));
        }

        Ok(Self { array })
    }

    fn starts_with(&self, arr: &dyn Array) -> Result<BooleanArray> {
        if arr.data_type() != self.array.data_type() {
            return Err(anyhow!(
                "filter array is of type {} but array to be filtered is of type {}",
                self.array.data_type(),
                arr.data_type(),
            ));
        }
        assert!(!self.array.is_nullable());

        let mut filter = match *arr.data_type() {
            DataType::Binary => {
                let self_arr = self.array.as_any().downcast_ref::<BinaryArray>().unwrap();
                let other_arr = arr.as_any().downcast_ref().unwrap();
                self.starts_with_bytes(self_arr, other_arr)
            }
            DataType::Utf8 => {
                let self_arr = self.array.as_any().downcast_ref::<StringArray>().unwrap();
                let other_arr = arr.as_any().downcast_ref().unwrap();
                self.starts_with_bytes(self_arr, other_arr)
            }
            _ => {
                return Err(anyhow!("unsupported data type: {}", arr.data_type()));
            }
        };

        if let Some(nulls) = arr.nulls() {
            if nulls.null_count() > 0 {
                let nulls = BooleanArray::from(nulls.inner().clone());
                filter = compute::and(&filter, &nulls).unwrap();
            }
        }

        Ok(filter)
    }

    fn starts_with_bytes<T: ByteArrayType<Offset = i32>>(
        &self,
        self_arr: &GenericByteArray<T>,
        other_arr: &GenericByteArray<T>,
    ) -> BooleanArray {
        let mut filter = BooleanBuilder::with_capacity(other_arr.len());

        // For each value in other_arr, check if it starts with any value in self_arr
        for v in iter_byte_array_without_validity(other_arr) {
            let mut found = false;
            for prefix in iter_byte_array_without_validity(self_arr) {
                if v.starts_with(prefix) {
                    found = true;
                    break;
                }
            }
            filter.append_value(found);
        }

        filter.finish()
    }
}

// Taken from arrow-rs
// https://docs.rs/arrow-array/54.2.1/src/arrow_array/array/byte_array.rs.html#278
unsafe fn byte_array_get_unchecked<T: ByteArrayType<Offset = i32>>(
    arr: &GenericByteArray<T>,
    i: usize,
) -> &[u8] {
    let end = *arr.value_offsets().get_unchecked(i + 1);
    let start = *arr.value_offsets().get_unchecked(i);

    std::slice::from_raw_parts(
        arr.value_data()
            .as_ptr()
            .offset(isize::try_from(start).unwrap()),
        usize::try_from(end - start).unwrap(),
    )
}

fn iter_byte_array_without_validity<T: ByteArrayType<Offset = i32>>(
    arr: &GenericByteArray<T>,
) -> impl Iterator<Item = &[u8]> {
    (0..arr.len()).map(|i| unsafe { byte_array_get_unchecked(arr, i) })
}

pub fn run_query(
    data: &BTreeMap<TableName, RecordBatch>,
    query: &Query,
) -> Result<BTreeMap<TableName, RecordBatch>> {
    let filters = query
        .selection
        .par_iter()
        .map(|(table_name, selections)| {
            selections
                .par_iter()
                .enumerate()
                .map(|(i, selection)| {
                    run_table_selection(data, table_name, selection).with_context(|| {
                        format!("run table selection no:{} for table {}", i, table_name)
                    })
                })
                .collect::<Result<Vec<_>>>()
        })
        .collect::<Result<Vec<_>>>()?;

    let data = select_fields(data, &query.fields).context("select fields")?;

    data.par_iter()
        .filter_map(|(table_name, table_data)| {
            let mut combined_filter: Option<BooleanArray> = None;

            for f in filters.iter() {
                for f in f.iter() {
                    let filter = match f.get(table_name) {
                        Some(f) => f,
                        None => continue,
                    };

                    match combined_filter.as_ref() {
                        Some(e) => {
                            let f = compute::or(e, filter)
                                .with_context(|| format!("combine filters for {}", table_name));
                            let f = match f {
                                Ok(v) => v,
                                Err(err) => return Some(Err(err)),
                            };
                            combined_filter = Some(f);
                        }
                        None => {
                            combined_filter = Some(filter.clone());
                        }
                    }
                }
            }

            let combined_filter = match combined_filter {
                Some(f) => f,
                None => return None,
            };

            let table_data = compute::filter_record_batch(table_data, &combined_filter)
                .context("filter record batch");
            let table_data = match table_data {
                Ok(v) => v,
                Err(err) => return Some(Err(err)),
            };

            Some(Ok((table_name.to_owned(), table_data)))
        })
        .collect()
}

pub fn select_fields(
    data: &BTreeMap<TableName, RecordBatch>,
    fields: &BTreeMap<TableName, Vec<FieldName>>,
) -> Result<BTreeMap<TableName, RecordBatch>> {
    let mut out = BTreeMap::new();

    for (table_name, field_names) in fields.iter() {
        let table_data = data
            .get(table_name)
            .with_context(|| format!("get data for table {}", table_name))?;

        let indices = table_data
            .schema_ref()
            .fields()
            .iter()
            .enumerate()
            .filter(|(_, field)| field_names.contains(field.name()))
            .map(|(i, _)| i)
            .collect::<Vec<usize>>();

        let table_data = table_data
            .project(&indices)
            .with_context(|| format!("project table {}", table_name))?;
        out.insert(table_name.to_owned(), table_data);
    }

    Ok(out)
}

fn run_table_selection(
    data: &BTreeMap<TableName, RecordBatch>,
    table_name: &str,
    selection: &TableSelection,
) -> Result<BTreeMap<TableName, BooleanArray>> {
    let mut out = BTreeMap::new();

    let table_data = data.get(table_name).context("get table data")?;
    let mut combined_filter = None;
    for (field_name, filter) in selection.filters.iter() {
        let col = table_data
            .column_by_name(field_name)
            .with_context(|| format!("get field {}", field_name))?;

        let f = filter
            .check(&col)
            .with_context(|| format!("check filter for column {}", field_name))?;

        match combined_filter {
            Some(cf) => {
                combined_filter = Some(
                    compute::and(&cf, &f)
                        .with_context(|| format!("combine filter for column {}", field_name))?,
                );
            }
            None => {
                combined_filter = Some(f);
            }
        }
    }

    let combined_filter = match combined_filter {
        Some(cf) => cf,
        None => BooleanArray::new(BooleanBuffer::new_set(table_data.num_rows()), None),
    };

    out.insert(table_name.to_owned(), combined_filter.clone());

    let mut filtered_cache = BTreeMap::new();

    for (i, inc) in selection.include.iter().enumerate() {
        if inc.other_table_field_names.len() != inc.field_names.len() {
            return Err(anyhow!(
                "field names are different for self table and other table while processing include no: {}. {} {}",
                i,
                inc.field_names.len(),
                inc.other_table_field_names.len(),
            ));
        }

        let other_table_data = data.get(&inc.other_table_name).with_context(|| {
            format!(
                "get data for table {} as other table data",
                inc.other_table_name
            )
        })?;

        let self_arr = columns_to_binary_array(table_data, &inc.field_names)
            .context("get row format binary arr for self")?;

        let contains = match filtered_cache.entry(inc.field_names.clone()) {
            Entry::Vacant(entry) => {
                let self_arr = compute::filter(&self_arr, &combined_filter)
                    .context("apply combined filter to self arr")?;
                let contains =
                    Contains::new(Arc::new(self_arr)).context("create contains filter")?;
                let contains = Arc::new(contains);
                entry.insert(Arc::clone(&contains));
                contains
            }
            Entry::Occupied(entry) => Arc::clone(entry.get()),
        };

        let other_arr = columns_to_binary_array(other_table_data, &inc.other_table_field_names)
            .with_context(|| {
                format!(
                    "get row format binary arr for other table {}",
                    inc.other_table_name
                )
            })?;

        let f = contains
            .contains(&other_arr)
            .with_context(|| format!("run contains for other table {}", inc.other_table_name))?;

        match out.entry(inc.other_table_name.clone()) {
            Entry::Vacant(entry) => {
                entry.insert(f);
            }
            Entry::Occupied(mut entry) => {
                let new = compute::or(entry.get(), &f).with_context(|| {
                    format!("or include filters for table {}", inc.other_table_name)
                })?;
                entry.insert(new);
            }
        }
    }

    Ok(out)
}

fn columns_to_binary_array(
    table_data: &RecordBatch,
    column_names: &[String],
) -> Result<BinaryArray> {
    let fields = column_names
        .iter()
        .map(|field_name| {
            let f = table_data
                .schema_ref()
                .field_with_name(field_name)
                .with_context(|| format!("get field {} from schema", field_name))?;
            Ok(SortField::new(f.data_type().clone()))
        })
        .collect::<Result<Vec<_>>>()?;
    let conv = RowConverter::new(fields).context("create row converter")?;

    let columns = column_names
        .iter()
        .map(|field_name| {
            let c = table_data
                .column_by_name(field_name)
                .with_context(|| format!("get data for column {}", field_name))?;
            let c = Arc::clone(c);
            Ok(c)
        })
        .collect::<Result<Vec<_>>>()?;

    let rows = conv
        .convert_columns(&columns)
        .context("convert columns to row format")?;
    let out = rows
        .try_into_binary()
        .context("convert row format to binary array")?;

    Ok(out)
}

#[cfg(test)]
mod tests {
    use arrow::{
        array::AsArray,
        datatypes::{Field, Schema},
    };

    use super::*;

    #[test]
    fn basic_test_cherry_query() {
        let team_a = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Arc::new(Field::new("name", DataType::Utf8, true)),
                Arc::new(Field::new("age", DataType::UInt64, true)),
                Arc::new(Field::new("height", DataType::UInt64, true)),
            ])),
            vec![
                Arc::new(StringArray::from_iter_values(
                    vec!["kamil", "mahmut", "qwe", "kazim"].into_iter(),
                )),
                Arc::new(UInt64Array::from_iter(vec![11, 12, 13, 31].into_iter())),
                Arc::new(UInt64Array::from_iter(vec![50, 60, 70, 60].into_iter())),
            ],
        )
        .unwrap();
        let team_b = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Arc::new(Field::new("name2", DataType::Utf8, true)),
                Arc::new(Field::new("age2", DataType::UInt64, true)),
                Arc::new(Field::new("height2", DataType::UInt64, true)),
            ])),
            vec![
                Arc::new(StringArray::from_iter_values(vec![
                    "yusuf", "abuzer", "asd",
                ])),
                Arc::new(UInt64Array::from_iter(vec![11, 12, 13].into_iter())),
                Arc::new(UInt64Array::from_iter(vec![50, 61, 70].into_iter())),
            ],
        )
        .unwrap();

        let query = Query {
            fields: [
                ("team_a".to_owned(), vec!["name".to_owned()]),
                ("team_b".to_owned(), vec!["name2".to_owned()]),
            ]
            .into_iter()
            .collect(),
            selection: Arc::new(
                [(
                    "team_a".to_owned(),
                    vec![TableSelection {
                        filters: [(
                            "name".to_owned(),
                            Filter::Contains(
                                Contains::new(Arc::new(StringArray::from_iter_values(
                                    vec!["kamil", "mahmut"].into_iter(),
                                )))
                                .unwrap(),
                            ),
                        )]
                        .into_iter()
                        .collect(),
                        include: vec![
                            Include {
                                field_names: vec!["age".to_owned(), "height".to_owned()],
                                other_table_field_names: vec![
                                    "age2".to_owned(),
                                    "height2".to_owned(),
                                ],
                                other_table_name: "team_b".to_owned(),
                            },
                            Include {
                                field_names: vec!["height".to_owned()],
                                other_table_field_names: vec!["height".to_owned()],
                                other_table_name: "team_a".to_owned(),
                            },
                        ],
                    }],
                )]
                .into_iter()
                .collect(),
            ),
        };

        let data = [("team_a".to_owned(), team_a), ("team_b".to_owned(), team_b)]
            .into_iter()
            .collect::<BTreeMap<_, _>>();

        let res = run_query(&data, &query).unwrap();

        let team_a = res.get("team_a").unwrap();
        let team_b = res.get("team_b").unwrap();

        assert_eq!(res.len(), 2);

        let name = team_a.column_by_name("name").unwrap();
        let name2 = team_b.column_by_name("name2").unwrap();

        assert_eq!(team_a.num_columns(), 1);
        assert_eq!(team_b.num_columns(), 1);

        assert_eq!(
            name.as_string(),
            &StringArray::from_iter_values(["kamil", "mahmut", "kazim"])
        );
        assert_eq!(name2.as_string(), &StringArray::from_iter_values(["yusuf"]));
    }

    #[test]
    fn test_starts_with_filter() {
        let data = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Arc::new(Field::new("name", DataType::Utf8, true)),
                Arc::new(Field::new("binary", DataType::Binary, true)),
            ])),
            vec![
                Arc::new(StringArray::from_iter_values(
                    vec!["hello", "world", "helloworld", "goodbye", "hell"].into_iter(),
                )),
                Arc::new(BinaryArray::from_iter_values(
                    vec![b"hello", b"world", b"hepto", b"grace", b"heheh"].into_iter(),
                )),
            ],
        )
        .unwrap();

        let query = Query {
            fields: [(
                "data".to_owned(),
                vec!["name".to_owned(), "binary".to_owned()],
            )]
            .into_iter()
            .collect(),
            selection: Arc::new(
                [(
                    "data".to_owned(),
                    vec![TableSelection {
                        filters: [
                            (
                                "name".to_owned(),
                                Filter::StartsWith(
                                    StartsWith::new(Arc::new(StringArray::from_iter_values(
                                        vec!["he"].into_iter(),
                                    )))
                                    .unwrap(),
                                ),
                            ),
                            (
                                "binary".to_owned(),
                                Filter::StartsWith(
                                    StartsWith::new(Arc::new(BinaryArray::from_iter_values(
                                        vec![b"he"].into_iter(),
                                    )))
                                    .unwrap(),
                                ),
                            ),
                        ]
                        .into_iter()
                        .collect(),
                        include: vec![],
                    }],
                )]
                .into_iter()
                .collect(),
            ),
        };

        let data = [("data".to_owned(), data)]
            .into_iter()
            .collect::<BTreeMap<_, _>>();

        let res = run_query(&data, &query).unwrap();
        let filtered = res.get("data").unwrap();

        let name = filtered.column_by_name("name").unwrap();
        let binary = filtered.column_by_name("binary").unwrap();
        assert_eq!(
            name.as_string(),
            &StringArray::from_iter_values(["hello", "helloworld", "hell"])
        );
        assert_eq!(
            binary.as_binary::<i32>(),
            &BinaryArray::from_iter_values([b"hello", b"hepto", b"heheh"].into_iter())
        );
    }
}
