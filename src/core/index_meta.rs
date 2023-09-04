use std::collections::HashSet;
use std::fmt;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use super::SegmentComponent;
use crate::core::SegmentId;
use crate::schema::Schema;
use crate::store::Compressor;
use crate::{Inventory, Opstamp, TrackedObject};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "icp", derive(candid::CandidType))]
struct DeleteMeta {
    num_deleted_docs: u32,
    opstamp: Opstamp,
}

#[cfg(feature = "icp")]
#[test]
fn ser_de_should_work_for_delete_meta() {
    let res = DeleteMeta {
        num_deleted_docs: 0,
        opstamp: 100,
    };

    let res = candid::encode_one(res).unwrap();
    let _: DeleteMeta = candid::decode_one(&res).unwrap();
}

#[derive(Clone, Default)]
pub struct SegmentMetaInventory {
    inventory: Inventory<InnerSegmentMeta>,
}

impl SegmentMetaInventory {
    /// Lists all living `SegmentMeta` object at the time of the call.
    pub fn all(&self) -> Vec<SegmentMeta> {
        self.inventory
            .list()
            .into_iter()
            .map(SegmentMeta::from)
            .collect::<Vec<_>>()
    }

    pub fn new_segment_meta(&self, segment_id: SegmentId, max_doc: u32) -> SegmentMeta {
        let inner = InnerSegmentMeta {
            segment_id,
            max_doc,
            include_temp_doc_store: Arc::new(AtomicBool::new(true)),
            deletes: None,
        };
        SegmentMeta::from(self.inventory.track(inner))
    }
}

/// `SegmentMeta` contains simple meta information about a segment.
///
/// For instance the number of docs it contains,
/// how many are deleted, etc.
#[derive(Clone)]
pub struct SegmentMeta {
    tracked: TrackedObject<InnerSegmentMeta>,
}

impl<'de> Deserialize<'de> for SegmentMeta {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where D: serde::Deserializer<'de> {
        use serde::de::Visitor;

        #[derive(Deserialize)]
        #[serde(field_identifier, rename_all = "snake_case")]
        // The fields we try to deserialize for SegmentMeta struct
        enum Field {
            Tracked,
        }

        struct SegmentMetaVisitor;
        impl<'de> Visitor<'de> for SegmentMetaVisitor {
            type Value = SegmentMeta;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("expect InnerSegmentMeta struct.")
            }
            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where A: serde::de::MapAccess<'de> {
                use serde::de::Error;
                let mut tracked = None;

                while let Some(key) = map.next_key()? {
                    match key {
                        Field::Tracked => {
                            if tracked.is_some() {
                                return Err(Error::duplicate_field("tracked"));
                            }
                            tracked = Some(map.next_value()?);
                        }
                    }
                }
                let tracked: InnerSegmentMeta =
                    tracked.ok_or_else(|| Error::missing_field("tracked"))?;
                Ok(tracked.track(&SegmentMetaInventory::default()))
            }
        }
        const FIELDS: &'static [&'static str] = &["tracked"];

        let res = deserializer.deserialize_struct("SegmentMeta", FIELDS, SegmentMetaVisitor)?;

        Ok(res)
    }
}

#[cfg(feature = "icp")]
impl candid::CandidType for SegmentMeta {
    fn _ty() -> candid::types::Type {
        use candid::field;
        use candid::types::TypeInner;
        let res = TypeInner::Record(vec![field! {tracked: InnerSegmentMeta::ty()}]);
        res.into()
    }

    fn idl_serialize<S>(&self, serializer: S) -> Result<(), S::Error>
    where S: candid::types::Serializer {
        use candid::types::Compound;
        let mut res = serializer.serialize_struct()?;
        res.serialize_element(self.tracked.as_ref())
    }
}

#[cfg(feature = "icp")]
#[test]
fn ser_de_should_work_for_segment_meta() {
    let inv = SegmentMetaInventory::default();
    let res = inv.new_segment_meta(SegmentId::generate_random(), 100);
    let res = candid::encode_one(res).unwrap();
    let res: SegmentMeta = candid::decode_one(&res).unwrap();
    println!("{res:?}");
}

impl fmt::Debug for SegmentMeta {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        self.tracked.fmt(f)
    }
}

impl serde::Serialize for SegmentMeta {
    fn serialize<S>(
        &self,
        serializer: S,
    ) -> Result<<S as serde::Serializer>::Ok, <S as serde::Serializer>::Error>
    where
        S: serde::Serializer,
    {
        self.tracked.serialize(serializer)
    }
}

impl From<TrackedObject<InnerSegmentMeta>> for SegmentMeta {
    fn from(tracked: TrackedObject<InnerSegmentMeta>) -> SegmentMeta {
        SegmentMeta { tracked }
    }
}

impl SegmentMeta {
    // Creates a new `SegmentMeta` object.

    /// Returns the segment id.
    pub fn id(&self) -> SegmentId {
        self.tracked.segment_id
    }

    /// Removes the Component::TempStore from the alive list and
    /// therefore marks the temp docstore file to be deleted by
    /// the garbage collection.
    pub fn untrack_temp_docstore(&self) {
        self.tracked
            .include_temp_doc_store
            .store(false, std::sync::atomic::Ordering::Relaxed);
    }

    /// Returns the number of deleted documents.
    pub fn num_deleted_docs(&self) -> u32 {
        self.tracked
            .deletes
            .as_ref()
            .map(|delete_meta| delete_meta.num_deleted_docs)
            .unwrap_or(0u32)
    }

    /// Returns the list of files that
    /// are required for the segment meta.
    /// Note: Some of the returned files may not exist depending on the state of the segment.
    ///
    /// This is useful as the way tantivy removes files
    /// is by removing all files that have been created by tantivy
    /// and are not used by any segment anymore.
    pub fn list_files(&self) -> HashSet<PathBuf> {
        if self
            .tracked
            .include_temp_doc_store
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            SegmentComponent::iterator()
                .map(|component| self.relative_path(*component))
                .collect::<HashSet<PathBuf>>()
        } else {
            SegmentComponent::iterator()
                .filter(|comp| *comp != &SegmentComponent::TempStore)
                .map(|component| self.relative_path(*component))
                .collect::<HashSet<PathBuf>>()
        }
    }

    /// Returns the relative path of a component of our segment.
    ///
    /// It just joins the segment id with the extension
    /// associated with a segment component.
    pub fn relative_path(&self, component: SegmentComponent) -> PathBuf {
        let mut path = self.id().uuid_string();
        path.push_str(&match component {
            SegmentComponent::Postings => ".idx".to_string(),
            SegmentComponent::Positions => ".pos".to_string(),
            SegmentComponent::Terms => ".term".to_string(),
            SegmentComponent::Store => ".store".to_string(),
            SegmentComponent::TempStore => ".store.temp".to_string(),
            SegmentComponent::FastFields => ".fast".to_string(),
            SegmentComponent::FieldNorms => ".fieldnorm".to_string(),
            SegmentComponent::Delete => format!(".{}.del", self.delete_opstamp().unwrap_or(0)),
        });
        PathBuf::from(path)
    }

    /// Return the highest doc id + 1
    ///
    /// If there are no deletes, then num_docs = max_docs
    /// and all the doc ids contains in this segment
    /// are exactly (0..max_doc).
    pub fn max_doc(&self) -> u32 {
        self.tracked.max_doc
    }

    /// Return the number of documents in the segment.
    pub fn num_docs(&self) -> u32 {
        self.max_doc() - self.num_deleted_docs()
    }

    /// Returns the `Opstamp` of the last delete operation
    /// taken in account in this segment.
    pub fn delete_opstamp(&self) -> Option<Opstamp> {
        self.tracked
            .deletes
            .as_ref()
            .map(|delete_meta| delete_meta.opstamp)
    }

    /// Returns true iff the segment meta contains
    /// delete information.
    pub fn has_deletes(&self) -> bool {
        self.num_deleted_docs() > 0
    }

    /// Updates the max_doc value from the `SegmentMeta`.
    ///
    /// This method is only used when updating `max_doc` from 0
    /// as we finalize a fresh new segment.
    pub(crate) fn with_max_doc(self, max_doc: u32) -> SegmentMeta {
        assert_eq!(self.tracked.max_doc, 0);
        assert!(self.tracked.deletes.is_none());
        let tracked = self.tracked.map(move |inner_meta| InnerSegmentMeta {
            segment_id: inner_meta.segment_id,
            max_doc,
            deletes: None,
            include_temp_doc_store: Arc::new(AtomicBool::new(true)),
        });
        SegmentMeta { tracked }
    }

    #[doc(hidden)]
    #[must_use]
    pub fn with_delete_meta(self, num_deleted_docs: u32, opstamp: Opstamp) -> SegmentMeta {
        assert!(
            num_deleted_docs <= self.max_doc(),
            "There cannot be more deleted docs than there are docs."
        );
        let delete_meta = DeleteMeta {
            num_deleted_docs,
            opstamp,
        };
        let tracked = self.tracked.map(move |inner_meta| InnerSegmentMeta {
            segment_id: inner_meta.segment_id,
            max_doc: inner_meta.max_doc,
            include_temp_doc_store: Arc::new(AtomicBool::new(true)),
            deletes: Some(delete_meta),
        });
        SegmentMeta { tracked }
    }
}

#[cfg(feature = "icp")]
#[test]
fn inner_segment_meta_ser_de_should_work() {
    let res = InnerSegmentMeta {
        segment_id: SegmentId::generate_random(),
        max_doc: 100,
        include_temp_doc_store: Arc::new(AtomicBool::new(true)),
        deletes: None,
    };
    let res = candid::encode_one(res).unwrap();
    let _: InnerSegmentMeta = candid::decode_one(&res).unwrap();
}

#[derive(Debug, Clone, Serialize)]
struct InnerSegmentMeta {
    segment_id: SegmentId,
    max_doc: u32,
    deletes: Option<DeleteMeta>,
    /// If you want to avoid the SegmentComponent::TempStore file to be covered by
    /// garbage collection and deleted, set this to true. This is used during merge.
    #[serde(skip)]
    #[cfg_attr(not(feature = "icp"), serde(default = "default_temp_store"))]
    pub(crate) include_temp_doc_store: Arc<AtomicBool>,
}

impl<'de> Deserialize<'de> for InnerSegmentMeta {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where D: serde::Deserializer<'de> {
        use serde::de::Visitor;

        #[derive(Deserialize)]
        #[serde(field_identifier, rename_all = "snake_case")]
        enum Field {
            SegmentId,
            MaxDoc,
            Deletes,
            IncludeTempDocStore,
        }
        struct InnerSegmentMetaVisitor;
        impl<'de> Visitor<'de> for InnerSegmentMetaVisitor {
            type Value = InnerSegmentMeta;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("expect struct InnerSegmentMeta.")
            }
            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where A: serde::de::MapAccess<'de> {
                use serde::de::Error;

                let mut segment_id = None;
                let mut max_doc = None;
                let mut deletes = None;
                let mut include_temp_doc_store = None;

                while let Some(key) = map.next_key()? {
                    match key {
                        Field::SegmentId => {
                            if segment_id.is_some() {
                                return Err(Error::duplicate_field("segment_id"));
                            }
                            segment_id = Some(map.next_value()?);
                        }
                        Field::MaxDoc => {
                            if max_doc.is_some() {
                                return Err(Error::duplicate_field("max_doc"));
                            }
                            max_doc = Some(map.next_value()?);
                        }
                        Field::Deletes => {
                            if deletes.is_some() {
                                return Err(Error::duplicate_field("max_doc"));
                            }
                            deletes = Some(map.next_value()?);
                        }
                        Field::IncludeTempDocStore => {
                            if include_temp_doc_store.is_some() {
                                return Err(Error::duplicate_field("max_doc"));
                            }
                            include_temp_doc_store = Some(map.next_value()?);
                        }
                    }
                }

                let segment_id = segment_id.ok_or_else(|| Error::missing_field("segment_id"))?;
                let max_doc = max_doc.ok_or_else(|| Error::missing_field("max_doc"))?;
                let deletes = deletes.ok_or_else(|| Error::missing_field("deletes"))?;
                // let include_temp_doc_store = include_temp_doc_store.ok_or_else(||
                // serde::de::Error::missing_field("include_temp_doc_store"))?;
                // We use default value for include_temp_doc_store here.
                let include_temp_doc_store = include_temp_doc_store.unwrap_or(false);
                Ok(InnerSegmentMeta {
                    segment_id,
                    max_doc,
                    deletes,
                    include_temp_doc_store: Arc::new(AtomicBool::new(include_temp_doc_store)),
                })
            }
        }
        const FIELDS: &'static [&'static str] =
            &["segment_id", "max_doc", "deletes", "include_temp_doc_store"];

        let res =
            deserializer.deserialize_struct("InnerSegmentMeta", FIELDS, InnerSegmentMetaVisitor)?;
        Ok(res)
    }
}

#[cfg(feature = "icp")]
impl candid::CandidType for InnerSegmentMeta {
    fn _ty() -> candid::types::Type {
        use candid::field;
        use candid::types::TypeInner;
        let res = TypeInner::Record(vec![
            field! {deletes: Option::<DeleteMeta>::_ty()},
            field! {segment_id: SegmentId::ty()},
            field! {include_temp_doc_store: bool::ty()},
            field! {max_doc: u32::ty()},
        ]);

        res.into()
    }

    fn idl_serialize<S>(&self, serializer: S) -> Result<(), S::Error>
    where S: candid::types::Serializer {
        use candid::types::Compound;
        let mut res = serializer.serialize_struct()?;

        res.serialize_element(&self.deletes)?;
        res.serialize_element(&self.segment_id)?;
        res.serialize_element(
            &self
                .include_temp_doc_store
                .load(std::sync::atomic::Ordering::SeqCst),
        )?;
        res.serialize_element(&self.max_doc)?;
        Ok(())
    }
}

#[cfg(not(feature = "icp"))]
fn default_temp_store() -> Arc<AtomicBool> {
    Arc::new(AtomicBool::new(false))
}

impl InnerSegmentMeta {
    pub fn track(self, inventory: &SegmentMetaInventory) -> SegmentMeta {
        SegmentMeta {
            tracked: inventory.inventory.track(self),
        }
    }
}

fn return_true() -> bool {
    true
}

fn is_true(val: &bool) -> bool {
    *val
}

/// Search Index Settings.
///
/// Contains settings which are applied on the whole
/// index, like presort documents.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[cfg_attr(feature = "icp", derive(candid::CandidType))]
pub struct IndexSettings {
    /// Sorts the documents by information
    /// provided in `IndexSortByField`
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sort_by_field: Option<IndexSortByField>,
    /// The `Compressor` used to compress the doc store.
    #[serde(default)]
    pub docstore_compression: Compressor,
    /// If set to true, docstore compression will happen on a dedicated thread.
    /// (defaults: true)
    #[doc(hidden)]
    #[serde(default = "return_true")]
    #[serde(skip_serializing_if = "is_true")]
    pub docstore_compress_dedicated_thread: bool,
    #[serde(default = "default_docstore_blocksize")]
    /// The size of each block that will be compressed and written to disk
    pub docstore_blocksize: usize,
}

#[cfg(feature = "icp")]
#[test]
fn ser_de_should_work_for_index_settings() {
    let res = IndexSettings::default();
    let res = candid::encode_one(res).unwrap();
    let _: IndexSettings = candid::decode_one(&res).unwrap();
}

/// Must be a function to be compatible with serde defaults
fn default_docstore_blocksize() -> usize {
    16_384
}

impl Default for IndexSettings {
    fn default() -> Self {
        Self {
            sort_by_field: None,
            docstore_compression: Compressor::default(),
            docstore_blocksize: default_docstore_blocksize(),
            docstore_compress_dedicated_thread: true,
        }
    }
}

/// Settings to presort the documents in an index
///
/// Presorting documents can greatly improve performance
/// in some scenarios, by applying top n
/// optimizations.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[cfg_attr(feature = "icp", derive(candid::CandidType))]
pub struct IndexSortByField {
    /// The field to sort the documents by
    pub field: String,
    /// The order to sort the documents by
    pub order: Order,
}

#[cfg(feature = "icp")]
#[test]
fn ser_de_should_work_for_index_sort_by_field() {
    let res = IndexSortByField {
        field: "test_field".into(),
        order: Order::Asc,
    };
    let res = candid::encode_one(res).unwrap();
    let _: IndexSortByField = candid::decode_one(&res).unwrap();
}

/// The order to sort by
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[cfg_attr(feature = "icp", derive(candid::CandidType))]
pub enum Order {
    /// Ascending Order
    Asc,
    /// Descending Order
    Desc,
}
impl Order {
    /// return if the Order is ascending
    pub fn is_asc(&self) -> bool {
        self == &Order::Asc
    }
    /// return if the Order is descending
    pub fn is_desc(&self) -> bool {
        self == &Order::Desc
    }
}

/// Meta information about the `Index`.
///
/// This object is serialized on disk in the `meta.json` file.
/// It keeps information about
/// * the searchable segments,
/// * the index `docstamp`
/// * the schema
#[derive(Clone, Serialize)]
#[cfg_attr(feature = "icp", derive(candid::CandidType, candid::Deserialize))]
pub struct IndexMeta {
    /// `IndexSettings` to configure index options.
    #[serde(default)]
    pub index_settings: IndexSettings,
    /// List of `SegmentMeta` information associated with each finalized segment of the index.
    pub segments: Vec<SegmentMeta>,
    /// Index `Schema`
    pub schema: Schema,
    /// Opstamp associated with the last `commit` operation.
    pub opstamp: Opstamp,
    /// Payload associated with the last commit.
    ///
    /// Upon commit, clients can optionally add a small `String` payload to their commit
    /// to help identify this commit.
    /// This payload is entirely unused by tantivy.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload: Option<String>,
}

#[cfg(feature = "icp")]
#[test]
fn index_meta_ser_de_for_candid_type_should_work() {
    let schema = Schema::builder().build();
    let res = IndexMeta::with_schema(schema);

    let res = candid::encode_one(res).unwrap();

    let _: IndexMeta = candid::decode_one(&res).unwrap();
}

#[derive(Deserialize, Debug)]
struct UntrackedIndexMeta {
    pub segments: Vec<InnerSegmentMeta>,
    #[serde(default)]
    pub index_settings: IndexSettings,
    pub schema: Schema,
    pub opstamp: Opstamp,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload: Option<String>,
}

impl UntrackedIndexMeta {
    pub fn track(self, inventory: &SegmentMetaInventory) -> IndexMeta {
        IndexMeta {
            index_settings: self.index_settings,
            segments: self
                .segments
                .into_iter()
                .map(|inner_seg_meta| inner_seg_meta.track(inventory))
                .collect::<Vec<SegmentMeta>>(),
            schema: self.schema,
            opstamp: self.opstamp,
            payload: self.payload,
        }
    }
}

impl IndexMeta {
    /// Create an `IndexMeta` object representing a brand new `Index`
    /// with the given index.
    ///
    /// This new index does not contains any segments.
    /// Opstamp will the value `0u64`.
    pub fn with_schema(schema: Schema) -> IndexMeta {
        IndexMeta {
            index_settings: IndexSettings::default(),
            segments: vec![],
            schema,
            opstamp: 0u64,
            payload: None,
        }
    }

    pub(crate) fn deserialize(
        meta_json: &str,
        inventory: &SegmentMetaInventory,
    ) -> serde_json::Result<IndexMeta> {
        let untracked_meta_json: UntrackedIndexMeta = serde_json::from_str(meta_json)?;
        Ok(untracked_meta_json.track(inventory))
    }
}

impl fmt::Debug for IndexMeta {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}",
            serde_json::ser::to_string(self)
                .expect("JSON serialization for IndexMeta should never fail.")
        )
    }
}

#[cfg(test)]
mod tests {

    use super::IndexMeta;
    use crate::core::index_meta::UntrackedIndexMeta;
    use crate::schema::{Schema, TEXT};
    #[cfg(feature = "lz4-compression")]
    use crate::store::Compressor;
    #[cfg(feature = "zstd-compression")]
    use crate::store::ZstdCompressor;
    use crate::{IndexSettings, IndexSortByField, Order};

    #[cfg(feature = "lz4-compression")]
    #[test]
    fn test_serialize_metas() {
        let schema = {
            let mut schema_builder = Schema::builder();
            schema_builder.add_text_field("text", TEXT);
            schema_builder.build()
        };
        let index_metas = IndexMeta {
            index_settings: IndexSettings {
                sort_by_field: Some(IndexSortByField {
                    field: "text".to_string(),
                    order: Order::Asc,
                }),
                ..Default::default()
            },
            segments: Vec::new(),
            schema,
            opstamp: 0u64,
            payload: None,
        };
        let json = serde_json::ser::to_string(&index_metas).expect("serialization failed");
        assert_eq!(
            json,
            r#"{"index_settings":{"sort_by_field":{"field":"text","order":"Asc"},"docstore_compression":"lz4","docstore_blocksize":16384},"segments":[],"schema":[{"name":"text","type":"text","options":{"indexing":{"record":"position","fieldnorms":true,"tokenizer":"default"},"stored":false,"fast":false}}],"opstamp":0}"#
        );

        let deser_meta: UntrackedIndexMeta = serde_json::from_str(&json).unwrap();
        assert_eq!(index_metas.index_settings, deser_meta.index_settings);
        assert_eq!(index_metas.schema, deser_meta.schema);
        assert_eq!(index_metas.opstamp, deser_meta.opstamp);
    }

    #[test]
    #[cfg(feature = "zstd-compression")]
    fn test_serialize_metas_zstd_compressor() {
        let schema = {
            let mut schema_builder = Schema::builder();
            schema_builder.add_text_field("text", TEXT);
            schema_builder.build()
        };
        let index_metas = IndexMeta {
            index_settings: IndexSettings {
                sort_by_field: Some(IndexSortByField {
                    field: "text".to_string(),
                    order: Order::Asc,
                }),
                docstore_compression: crate::store::Compressor::Zstd(ZstdCompressor {
                    compression_level: Some(4),
                }),
                docstore_blocksize: 1_000_000,
                docstore_compress_dedicated_thread: true,
            },
            segments: Vec::new(),
            schema,
            opstamp: 0u64,
            payload: None,
        };
        let json = serde_json::ser::to_string(&index_metas).expect("serialization failed");
        assert_eq!(
            json,
            r#"{"index_settings":{"sort_by_field":{"field":"text","order":"Asc"},"docstore_compression":"zstd(compression_level=4)","docstore_blocksize":1000000},"segments":[],"schema":[{"name":"text","type":"text","options":{"indexing":{"record":"position","fieldnorms":true,"tokenizer":"default"},"stored":false,"fast":false}}],"opstamp":0}"#
        );

        let deser_meta: UntrackedIndexMeta = serde_json::from_str(&json).unwrap();
        assert_eq!(index_metas.index_settings, deser_meta.index_settings);
        assert_eq!(index_metas.schema, deser_meta.schema);
        assert_eq!(index_metas.opstamp, deser_meta.opstamp);
    }

    #[test]
    #[cfg(all(feature = "lz4-compression", feature = "zstd-compression"))]
    fn test_serialize_metas_invalid_comp() {
        let json = r#"{"index_settings":{"sort_by_field":{"field":"text","order":"Asc"},"docstore_compression":"zsstd","docstore_blocksize":1000000},"segments":[],"schema":[{"name":"text","type":"text","options":{"indexing":{"record":"position","fieldnorms":true,"tokenizer":"default"},"stored":false,"fast":false}}],"opstamp":0}"#;

        let err = serde_json::from_str::<UntrackedIndexMeta>(json).unwrap_err();
        assert_eq!(
            err.to_string(),
            "unknown variant `zsstd`, expected one of `none`, `lz4`, `zstd`, \
             `zstd(compression_level=5)` at line 1 column 96"
                .to_string()
        );

        let json = r#"{"index_settings":{"sort_by_field":{"field":"text","order":"Asc"},"docstore_compression":"zstd(bla=10)","docstore_blocksize":1000000},"segments":[],"schema":[{"name":"text","type":"text","options":{"indexing":{"record":"position","fieldnorms":true,"tokenizer":"default"},"stored":false,"fast":false}}],"opstamp":0}"#;

        let err = serde_json::from_str::<UntrackedIndexMeta>(json).unwrap_err();
        assert_eq!(
            err.to_string(),
            "unknown zstd option \"bla\" at line 1 column 103".to_string()
        );
    }

    #[test]
    #[cfg(not(feature = "zstd-compression"))]
    fn test_serialize_metas_unsupported_comp() {
        let json = r#"{"index_settings":{"sort_by_field":{"field":"text","order":"Asc"},"docstore_compression":"zstd","docstore_blocksize":1000000},"segments":[],"schema":[{"name":"text","type":"text","options":{"indexing":{"record":"position","fieldnorms":true,"tokenizer":"default"},"stored":false,"fast":false}}],"opstamp":0}"#;

        let err = serde_json::from_str::<UntrackedIndexMeta>(json).unwrap_err();
        assert_eq!(
            err.to_string(),
            "unsupported variant `zstd`, please enable Tantivy's `zstd-compression` feature at \
             line 1 column 95"
                .to_string()
        );
    }

    #[test]
    #[cfg(feature = "lz4-compression")]
    fn test_index_settings_default() {
        let mut index_settings = IndexSettings::default();
        assert_eq!(
            index_settings,
            IndexSettings {
                sort_by_field: None,
                docstore_compression: Compressor::default(),
                docstore_compress_dedicated_thread: true,
                docstore_blocksize: 16_384
            }
        );
        {
            let index_settings_json = serde_json::to_value(&index_settings).unwrap();
            assert_eq!(
                index_settings_json,
                serde_json::json!({
                    "docstore_compression": "lz4",
                    "docstore_blocksize": 16384
                })
            );
            let index_settings_deser: IndexSettings =
                serde_json::from_value(index_settings_json).unwrap();
            assert_eq!(index_settings_deser, index_settings);
        }
        {
            index_settings.docstore_compress_dedicated_thread = false;
            let index_settings_json = serde_json::to_value(&index_settings).unwrap();
            assert_eq!(
                index_settings_json,
                serde_json::json!({
                    "docstore_compression": "lz4",
                    "docstore_blocksize": 16384,
                    "docstore_compress_dedicated_thread": false,
                })
            );
            let index_settings_deser: IndexSettings =
                serde_json::from_value(index_settings_json).unwrap();
            assert_eq!(index_settings_deser, index_settings);
        }
    }
}
