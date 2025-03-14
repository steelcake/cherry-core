use anyhow::{anyhow, Context, Result};
use arrow::array::{
    Array, ArrowPrimitiveType, BinaryArray, BooleanArray, BooleanBuilder, GenericByteArray,
    Int16Array, Int32Array, Int64Array, Int8Array, PrimitiveArray, StringArray, UInt16Array,
    UInt32Array, UInt64Array, UInt8Array,
};
use arrow::compute;
use arrow::datatypes::{ByteArrayType, DataType, ToByteSlice};
use arrow::record_batch::RecordBatch;
use hashbrown::HashTable;
use rayon::prelude::*;
use std::collections::BTreeMap;
use std::sync::Arc;
use xxhash_rust::xxh3::xxh3_64;

type TableName = String;
type FieldName = String;

pub struct Query {
    pub selection: BTreeMap<TableName, Vec<TableSelection>>,
    pub fields: BTreeMap<TableName, Vec<FieldName>>,
}

pub struct TableSelection {
    pub filters: BTreeMap<FieldName, Vec<Filter>>,
    pub include: Vec<Include>,
}

pub struct Include {
    pub other_table_name: TableName,
    pub field_names: Vec<FieldName>,
    pub other_table_field_names: Vec<FieldName>,
}

pub enum Filter {
    Contains(Contains),
    Bool(bool),
}

impl Filter {
    pub fn contains(arr: Arc<dyn Array>) -> Result<Self> {
        Ok(Self::Contains(Contains::new(arr)?))
    }

    pub fn bool(b: bool) -> Self {
        Self::Bool(b)
    }

    fn check(&self, arr: &dyn Array) -> Result<BooleanArray> {
        match self {
            Self::Contains(ct) => ct.contains(arr),
            Self::Bool(b) => {
                todo!()
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
        let ht = match array.data_type() {
            &DataType::UInt8 => {
                let array = array.as_any().downcast_ref::<UInt8Array>().unwrap();
                Self::ht_from_primitive(array)
            }
            &DataType::UInt16 => {
                let array = array.as_any().downcast_ref::<UInt16Array>().unwrap();
                Self::ht_from_primitive(array)
            }
            &DataType::UInt32 => {
                let array = array.as_any().downcast_ref::<UInt32Array>().unwrap();
                Self::ht_from_primitive(array)
            }
            &DataType::UInt64 => {
                let array = array.as_any().downcast_ref::<UInt64Array>().unwrap();
                Self::ht_from_primitive(array)
            }
            &DataType::Int8 => {
                let array = array.as_any().downcast_ref::<Int8Array>().unwrap();
                Self::ht_from_primitive(array)
            }
            &DataType::Int16 => {
                let array = array.as_any().downcast_ref::<Int16Array>().unwrap();
                Self::ht_from_primitive(array)
            }
            &DataType::Int32 => {
                let array = array.as_any().downcast_ref::<Int32Array>().unwrap();
                Self::ht_from_primitive(array)
            }
            &DataType::Int64 => {
                let array = array.as_any().downcast_ref::<Int64Array>().unwrap();
                Self::ht_from_primitive(array)
            }
            &DataType::Binary => {
                let array = array.as_any().downcast_ref::<BinaryArray>().unwrap();
                Self::ht_from_bytes(array)
            }
            &DataType::Utf8 => {
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

        let filter = match arr.data_type() {
            &DataType::UInt8 => {
                let self_arr = self.array.as_any().downcast_ref::<UInt8Array>().unwrap();
                let other_arr = arr.as_any().downcast_ref().unwrap();
                self.contains_primitive(self_arr, other_arr)
            }
            &DataType::UInt16 => {
                let self_arr = self.array.as_any().downcast_ref::<UInt16Array>().unwrap();
                let other_arr = arr.as_any().downcast_ref().unwrap();
                self.contains_primitive(self_arr, other_arr)
            }
            &DataType::UInt32 => {
                let self_arr = self.array.as_any().downcast_ref::<UInt32Array>().unwrap();
                let other_arr = arr.as_any().downcast_ref().unwrap();
                self.contains_primitive(self_arr, other_arr)
            }
            &DataType::UInt64 => {
                let self_arr = self.array.as_any().downcast_ref::<UInt64Array>().unwrap();
                let other_arr = arr.as_any().downcast_ref().unwrap();
                self.contains_primitive(self_arr, other_arr)
            }
            &DataType::Int8 => {
                let self_arr = self.array.as_any().downcast_ref::<Int8Array>().unwrap();
                let other_arr = arr.as_any().downcast_ref().unwrap();
                self.contains_primitive(self_arr, other_arr)
            }
            &DataType::Int16 => {
                let self_arr = self.array.as_any().downcast_ref::<Int16Array>().unwrap();
                let other_arr = arr.as_any().downcast_ref().unwrap();
                self.contains_primitive(self_arr, other_arr)
            }
            &DataType::Int32 => {
                let self_arr = self.array.as_any().downcast_ref::<Int32Array>().unwrap();
                let other_arr = arr.as_any().downcast_ref().unwrap();
                self.contains_primitive(self_arr, other_arr)
            }
            &DataType::Int64 => {
                let self_arr = self.array.as_any().downcast_ref::<Int64Array>().unwrap();
                let other_arr = arr.as_any().downcast_ref().unwrap();
                self.contains_primitive(self_arr, other_arr)
            }
            &DataType::Binary => {
                let self_arr = self.array.as_any().downcast_ref::<BinaryArray>().unwrap();
                let other_arr = arr.as_any().downcast_ref().unwrap();
                self.contains_bytes(self_arr, other_arr)
            }
            &DataType::Utf8 => {
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
                filter = compute::kernels::boolean::and(&filter, &nulls).unwrap();
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
                filter.append_value(self_arr.values().iter().find(|x| *x == v).is_some());
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
                filter.append_value(
                    iter_byte_array_without_validity(self_arr)
                        .find(|x| *x == v)
                        .is_some(),
                );
            }
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
                    run_table_selection(data, table_name, selection)
                        .with_context(|| format!("run table selection {} for {}", i, table_name))
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
                            let f = compute::kernels::boolean::or(e, &filter)
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

            let table_data =
                compute::kernels::filter::filter_record_batch(table_data, &combined_filter)
                    .context("filter record batch");
            let table_data = match table_data {
                Ok(v) => v,
                Err(err) => return Some(Err(err)),
            };

            Some(Ok((table_name.to_owned(), table_data)))
        })
        .collect()
}

fn select_fields(
    data: &BTreeMap<TableName, RecordBatch>,
    fields: &BTreeMap<TableName, Vec<FieldName>>,
) -> Result<BTreeMap<TableName, RecordBatch>> {
    let mut out = BTreeMap::new();

    for (table_name, field_names) in fields.iter() {
        let table_data = data
            .get(table_name)
            .with_context(|| format!("get data for table {}", table_name))?;

        let indices = field_names
            .iter()
            .map(|n| {
                table_data
                    .schema_ref()
                    .index_of(n)
                    .with_context(|| format!("find index of field {} in table {}", n, table_name))
            })
            .collect::<Result<Vec<usize>>>()?;

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
    todo!()
}
