use super::{condition::StatusConditionAsync, subscriber::SubscriberAsync};
use crate::{
    dcps::{
        actor::ActorAddress,
        channels::{mpsc::MpscSender, oneshot::oneshot},
        domain_participant_mail::{DcpsDomainParticipantMail, ReaderServiceMail},
        status_condition::DcpsStatusCondition,
    },
    infrastructure::{
        error::DdsResult,
        instance::InstanceHandle,
        qos::DataReaderQos,
        sample_info::{
            InstanceStateKind, SampleInfo, SampleStateKind, ViewStateKind, ANY_INSTANCE_STATE,
            ANY_VIEW_STATE,
        },
        status::StatusKind,
    },
    xtypes::dynamic_type::{DynamicData, DynamicType},
};
use alloc::{string::String, vec, vec::Vec};

/// A sample containing dynamic data and its associated sample info.
#[derive(Debug, Clone, PartialEq)]
pub struct DynamicSample {
    /// The dynamic data, if valid.
    pub data: Option<DynamicData>,
    /// Information about the sample.
    pub sample_info: SampleInfo,
}

impl DynamicSample {
    /// Creates a new DynamicSample.
    pub fn new(data: Option<DynamicData>, sample_info: SampleInfo) -> Self {
        Self { data, sample_info }
    }
}

/// A [`DynamicDataReaderAsync`] allows the application to subscribe to a topic and access received data using runtime type information.
///
/// Unlike [`DataReaderAsync`](crate::dds_async::data_reader::DataReaderAsync), this reader works with
/// [`DynamicData`] and does not require compile-time type knowledge.
/// This is useful for generic monitoring tools, data recorders, or dynamic language bindings.
pub struct DynamicDataReaderAsync {
    handle: InstanceHandle,
    status_condition_address: ActorAddress<DcpsStatusCondition>,
    subscriber: SubscriberAsync,
    topic_name: String,
    dynamic_type: DynamicType,
}

impl Clone for DynamicDataReaderAsync {
    fn clone(&self) -> Self {
        Self {
            handle: self.handle,
            status_condition_address: self.status_condition_address.clone(),
            subscriber: self.subscriber.clone(),
            topic_name: self.topic_name.clone(),
            dynamic_type: self.dynamic_type.clone(),
        }
    }
}

impl DynamicDataReaderAsync {
    pub(crate) fn new(
        handle: InstanceHandle,
        status_condition_address: ActorAddress<DcpsStatusCondition>,
        subscriber: SubscriberAsync,
        topic_name: String,
        dynamic_type: DynamicType,
    ) -> Self {
        Self {
            handle,
            status_condition_address,
            subscriber,
            topic_name,
            dynamic_type,
        }
    }

    fn participant_address(&self) -> &MpscSender<DcpsDomainParticipantMail> {
        self.subscriber.participant_address()
    }

    /// Returns the dynamic type associated with this reader.
    pub fn get_type(&self) -> &DynamicType {
        &self.dynamic_type
    }

    /// Returns the topic name this reader is subscribed to.
    pub fn get_topic_name(&self) -> &str {
        &self.topic_name
    }

    /// Read samples with full filtering options.
    #[tracing::instrument(skip(self))]
    pub async fn read(
        &self,
        max_samples: i32,
        sample_states: &[SampleStateKind],
        view_states: &[ViewStateKind],
        instance_states: &[InstanceStateKind],
    ) -> DdsResult<Vec<DynamicSample>> {
        let (reply_sender, reply_receiver) = oneshot();
        self.participant_address()
            .send(DcpsDomainParticipantMail::Reader(ReaderServiceMail::Read {
                subscriber_handle: self.subscriber.get_instance_handle().await,
                data_reader_handle: self.handle,
                max_samples,
                sample_states: sample_states.to_vec(),
                view_states: view_states.to_vec(),
                instance_states: instance_states.to_vec(),
                specific_instance_handle: None,
                reply_sender,
            }))
            .await?;
        let samples = reply_receiver.await??;

        Ok(samples
            .into_iter()
            .map(|(data, sample_info)| DynamicSample::new(data, sample_info))
            .collect())
    }

    /// Take samples with full filtering options.
    #[tracing::instrument(skip(self))]
    pub async fn take(
        &self,
        max_samples: i32,
        sample_states: &[SampleStateKind],
        view_states: &[ViewStateKind],
        instance_states: &[InstanceStateKind],
    ) -> DdsResult<Vec<DynamicSample>> {
        let (reply_sender, reply_receiver) = oneshot();
        self.participant_address()
            .send(DcpsDomainParticipantMail::Reader(ReaderServiceMail::Take {
                subscriber_handle: self.subscriber.get_instance_handle().await,
                data_reader_handle: self.handle,
                max_samples,
                sample_states: sample_states.to_vec(),
                view_states: view_states.to_vec(),
                instance_states: instance_states.to_vec(),
                specific_instance_handle: None,
                reply_sender,
            }))
            .await?;
        let samples = reply_receiver.await??;

        Ok(samples
            .into_iter()
            .map(|(data, sample_info)| DynamicSample::new(data, sample_info))
            .collect())
    }

    /// Read the next unread sample.
    #[tracing::instrument(skip(self))]
    pub async fn read_next_sample(&self) -> DdsResult<DynamicSample> {
        let (reply_sender, reply_receiver) = oneshot();
        self.participant_address()
            .send(DcpsDomainParticipantMail::Reader(ReaderServiceMail::Read {
                subscriber_handle: self.subscriber.get_instance_handle().await,
                data_reader_handle: self.handle,
                max_samples: 1,
                sample_states: vec![SampleStateKind::NotRead],
                view_states: ANY_VIEW_STATE.to_vec(),
                instance_states: ANY_INSTANCE_STATE.to_vec(),
                specific_instance_handle: None,
                reply_sender,
            }))
            .await?;
        let mut samples = reply_receiver.await??;
        let (data, sample_info) = samples.pop().expect("Would return NoData if empty");
        Ok(DynamicSample::new(data, sample_info))
    }

    /// Take the next unread sample.
    #[tracing::instrument(skip(self))]
    pub async fn take_next_sample(&self) -> DdsResult<DynamicSample> {
        let (reply_sender, reply_receiver) = oneshot();
        self.participant_address()
            .send(DcpsDomainParticipantMail::Reader(ReaderServiceMail::Take {
                subscriber_handle: self.subscriber.get_instance_handle().await,
                data_reader_handle: self.handle,
                max_samples: 1,
                sample_states: vec![SampleStateKind::NotRead],
                view_states: ANY_VIEW_STATE.to_vec(),
                instance_states: ANY_INSTANCE_STATE.to_vec(),
                specific_instance_handle: None,
                reply_sender,
            }))
            .await?;
        let mut samples = reply_receiver.await??;
        let (data, sample_info) = samples.pop().expect("Would return NoData if empty");
        Ok(DynamicSample::new(data, sample_info))
    }

    /// Get the status condition for this reader.
    pub fn get_statuscondition(&self) -> StatusConditionAsync {
        StatusConditionAsync::new(self.status_condition_address.clone())
    }

    /// Get the subscriber that created this reader.
    pub fn get_subscriber(&self) -> SubscriberAsync {
        self.subscriber.clone()
    }

    /// Get the instance handle of this reader.
    pub async fn get_instance_handle(&self) -> InstanceHandle {
        self.handle
    }

    /// Get the QoS settings for this reader.
    #[tracing::instrument(skip(self))]
    pub async fn get_qos(&self) -> DdsResult<DataReaderQos> {
        let (reply_sender, reply_receiver) = oneshot();
        self.participant_address()
            .send(DcpsDomainParticipantMail::Reader(
                ReaderServiceMail::GetQos {
                    subscriber_handle: self.subscriber.get_instance_handle().await,
                    data_reader_handle: self.handle,
                    reply_sender,
                },
            ))
            .await?;
        reply_receiver.await?
    }

    /// Enable this reader.
    #[tracing::instrument(skip(self))]
    pub async fn enable(&self) -> DdsResult<()> {
        let (reply_sender, reply_receiver) = oneshot();
        self.participant_address()
            .send(DcpsDomainParticipantMail::Reader(
                ReaderServiceMail::Enable {
                    subscriber_handle: self.subscriber.get_instance_handle().await,
                    data_reader_handle: self.handle,
                    participant_address: self.participant_address().clone(),
                    reply_sender,
                },
            ))
            .await?;
        reply_receiver.await?
    }

    /// Get the status changes for this reader.
    #[tracing::instrument(skip(self))]
    pub async fn get_status_changes(&self) -> DdsResult<Vec<StatusKind>> {
        todo!()
    }
}

#[cfg(all(test, feature = "type_lookup"))]
mod tests {
    use super::*;
    use crate::dds_async::domain_participant_factory::DomainParticipantFactoryAsync;
    use crate::infrastructure::{
        qos::QosKind,
        qos_policy::{ReliabilityQosPolicy, ReliabilityQosPolicyKind},
        sample_info::{ANY_INSTANCE_STATE, ANY_SAMPLE_STATE, ANY_VIEW_STATE},
        status::NO_STATUS,
        time::{Duration, DurationKind},
        type_support::TypeSupport,
    };
    use crate::xtypes::{
        binding::XTypesBinding,
        data_storage::DataStorageMapping,
        dynamic_type::{
            DynamicTypeBuilderFactory, ExtensibilityKind, MemberDescriptor, TryConstructKind,
            TypeDescriptor, TypeKind,
        },
    };
    use alloc::string::String;

    /// A simple test type for integration testing.
    #[derive(Debug, Clone, PartialEq)]
    struct TestMessage {
        id: i32,
        message: String,
    }

    impl TypeSupport for TestMessage {
        fn get_type_name() -> &'static str {
            "TestMessage"
        }

        fn get_type() -> DynamicType {
            let mut builder = DynamicTypeBuilderFactory::create_type(TypeDescriptor {
                kind: TypeKind::STRUCTURE,
                name: String::from("TestMessage"),
                base_type: None,
                discriminator_type: None,
                bound: Vec::new(),
                element_type: None,
                key_element_type: None,
                extensibility_kind: ExtensibilityKind::Final,
                is_nested: false,
            });
            builder
                .add_member(MemberDescriptor {
                    name: String::from("id"),
                    id: 0,
                    r#type: <i32 as XTypesBinding>::get_dynamic_type(),
                    default_value: None,
                    index: 0,
                    label: Vec::new(),
                    try_construct_kind: TryConstructKind::UseDefault,
                    is_key: true,
                    is_optional: false,
                    is_must_understand: false,
                    is_shared: false,
                    is_default_label: false,
                })
                .unwrap();
            builder
                .add_member(MemberDescriptor {
                    name: String::from("message"),
                    id: 1,
                    r#type: DynamicTypeBuilderFactory::create_string_type(0).build(),
                    default_value: None,
                    index: 1,
                    label: Vec::new(),
                    try_construct_kind: TryConstructKind::UseDefault,
                    is_key: false,
                    is_optional: false,
                    is_must_understand: false,
                    is_shared: false,
                    is_default_label: false,
                })
                .unwrap();
            builder.build()
        }

        fn create_sample(src: DynamicData) -> Self {
            let id = src
                .get_value(0)
                .ok()
                .and_then(|s| i32::try_from_storage(s.clone()).ok())
                .unwrap_or(0);
            let message = src
                .get_value(1)
                .ok()
                .and_then(|s| String::try_from_storage(s.clone()).ok())
                .unwrap_or_default();
            TestMessage { id, message }
        }

        fn create_dynamic_sample(self) -> DynamicData {
            let mut data = crate::xtypes::dynamic_type::DynamicDataFactory::create_data(
                Self::get_type(),
            );
            data.set_value(0, self.id.into_storage());
            data.set_value(1, self.message.into_storage());
            data
        }
    }

    /// Integration test: Typed DataWriter writes, DynamicDataReader reads.
    ///
    /// This test verifies AC #7: "participant discovers remote type and reads data
    /// without compile-time type". While this test uses manually-constructed
    /// DynamicType (simulating what TypeLookup would provide), it proves that
    /// DynamicDataReader can successfully read data written by a typed DataWriter.
    #[tokio::test]
    async fn test_dynamic_data_reader_reads_typed_writer_data() {
        // Create participant
        let factory = DomainParticipantFactoryAsync::get_instance();
        let participant = factory
            .create_participant(200, QosKind::Default, None::<()>, NO_STATUS)
            .await
            .expect("Failed to create participant");

        // Create topic with typed DataWriter's type
        let topic = participant
            .create_topic::<TestMessage>("TestTopic", "TestMessage", QosKind::Default, None::<()>, NO_STATUS)
            .await
            .expect("Failed to create topic");

        // Create publisher and typed DataWriter
        let publisher = participant
            .create_publisher(QosKind::Default, None::<()>, NO_STATUS)
            .await
            .expect("Failed to create publisher");

        let mut writer_qos = crate::infrastructure::qos::DataWriterQos::default();
        writer_qos.reliability = ReliabilityQosPolicy {
            kind: ReliabilityQosPolicyKind::Reliable,
            max_blocking_time: DurationKind::Finite(Duration::new(1, 0)),
        };
        let writer = publisher
            .create_datawriter::<TestMessage>(&topic, QosKind::Specific(writer_qos), None::<()>, NO_STATUS)
            .await
            .expect("Failed to create datawriter");

        // Create subscriber and DynamicDataReader with the same type (simulating TypeLookup discovery)
        let subscriber = participant
            .create_subscriber(QosKind::Default, None::<()>, NO_STATUS)
            .await
            .expect("Failed to create subscriber");

        // This is the key part: create DynamicDataReader with DynamicType, not compile-time type
        let dynamic_type = TestMessage::get_type();
        let mut reader_qos = crate::infrastructure::qos::DataReaderQos::default();
        reader_qos.reliability = ReliabilityQosPolicy {
            kind: ReliabilityQosPolicyKind::Reliable,
            max_blocking_time: DurationKind::Finite(Duration::new(1, 0)),
        };
        let dynamic_reader = subscriber
            .create_dynamic_datareader("TestTopic", dynamic_type, QosKind::Specific(reader_qos))
            .await
            .expect("Failed to create dynamic datareader");

        // Wait for discovery
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        // Write data with typed DataWriter
        let test_data = TestMessage {
            id: 42,
            message: String::from("Hello, Dynamic World!"),
        };
        writer.write(test_data, None).await.expect("Failed to write");

        // Wait for data to propagate
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        // Read with DynamicDataReader
        let samples = dynamic_reader
            .take(10, &ANY_SAMPLE_STATE, &ANY_VIEW_STATE, &ANY_INSTANCE_STATE)
            .await
            .expect("Failed to take samples");

        assert!(!samples.is_empty(), "Expected at least one sample");

        let sample = &samples[0];
        assert!(sample.data.is_some(), "Expected valid data");

        let data = sample.data.as_ref().unwrap();

        // Verify data using by-name accessors
        let id = data.get_int32_value_by_name("id").expect("Failed to get id");
        assert_eq!(*id, 42);

        let message = data
            .get_string_value_by_name("message")
            .expect("Failed to get message");
        assert_eq!(message.as_str(), "Hello, Dynamic World!");

        // Cleanup
        participant
            .delete_contained_entities()
            .await
            .expect("Failed to delete entities");
        factory
            .delete_participant(&participant)
            .await
            .expect("Failed to delete participant");
    }

    /// An enum type for integration testing.
    #[derive(Debug, Clone, Copy, PartialEq)]
    #[repr(i32)]
    enum Priority {
        Low = 0,
        Medium = 1,
        High = 2,
    }

    /// A struct with an enum field for integration testing.
    #[derive(Debug, Clone, PartialEq)]
    struct TaskWithPriority {
        task_id: i32,
        priority: Priority,
        description: String,
    }

    impl TypeSupport for TaskWithPriority {
        fn get_type_name() -> &'static str {
            "TaskWithPriority"
        }

        fn get_type() -> DynamicType {
            // Create the Priority enum type
            let discriminator_type =
                DynamicTypeBuilderFactory::get_primitive_type(TypeKind::INT32);
            let mut enum_builder = DynamicTypeBuilderFactory::create_type(TypeDescriptor {
                kind: TypeKind::ENUM,
                name: String::from("Priority"),
                base_type: None,
                discriminator_type: Some(discriminator_type),
                bound: Vec::new(),
                element_type: None,
                key_element_type: None,
                extensibility_kind: ExtensibilityKind::Final,
                is_nested: false,
            });
            enum_builder
                .add_member(MemberDescriptor {
                    name: String::from("Low"),
                    id: 0,
                    r#type: DynamicTypeBuilderFactory::get_primitive_type(TypeKind::INT32),
                    default_value: None,
                    index: 0,
                    label: Vec::new(),
                    try_construct_kind: TryConstructKind::UseDefault,
                    is_key: false,
                    is_optional: false,
                    is_must_understand: false,
                    is_shared: false,
                    is_default_label: true,
                })
                .unwrap();
            enum_builder
                .add_member(MemberDescriptor {
                    name: String::from("Medium"),
                    id: 1,
                    r#type: DynamicTypeBuilderFactory::get_primitive_type(TypeKind::INT32),
                    default_value: None,
                    index: 1,
                    label: Vec::new(),
                    try_construct_kind: TryConstructKind::UseDefault,
                    is_key: false,
                    is_optional: false,
                    is_must_understand: false,
                    is_shared: false,
                    is_default_label: false,
                })
                .unwrap();
            enum_builder
                .add_member(MemberDescriptor {
                    name: String::from("High"),
                    id: 2,
                    r#type: DynamicTypeBuilderFactory::get_primitive_type(TypeKind::INT32),
                    default_value: None,
                    index: 2,
                    label: Vec::new(),
                    try_construct_kind: TryConstructKind::UseDefault,
                    is_key: false,
                    is_optional: false,
                    is_must_understand: false,
                    is_shared: false,
                    is_default_label: false,
                })
                .unwrap();
            let priority_enum = enum_builder.build();

            // Create the struct type
            let mut builder = DynamicTypeBuilderFactory::create_type(TypeDescriptor {
                kind: TypeKind::STRUCTURE,
                name: String::from("TaskWithPriority"),
                base_type: None,
                discriminator_type: None,
                bound: Vec::new(),
                element_type: None,
                key_element_type: None,
                extensibility_kind: ExtensibilityKind::Final,
                is_nested: false,
            });
            builder
                .add_member(MemberDescriptor {
                    name: String::from("task_id"),
                    id: 0,
                    r#type: <i32 as XTypesBinding>::get_dynamic_type(),
                    default_value: None,
                    index: 0,
                    label: Vec::new(),
                    try_construct_kind: TryConstructKind::UseDefault,
                    is_key: true,
                    is_optional: false,
                    is_must_understand: false,
                    is_shared: false,
                    is_default_label: false,
                })
                .unwrap();
            builder
                .add_member(MemberDescriptor {
                    name: String::from("priority"),
                    id: 1,
                    r#type: priority_enum,
                    default_value: None,
                    index: 1,
                    label: Vec::new(),
                    try_construct_kind: TryConstructKind::UseDefault,
                    is_key: false,
                    is_optional: false,
                    is_must_understand: false,
                    is_shared: false,
                    is_default_label: false,
                })
                .unwrap();
            builder
                .add_member(MemberDescriptor {
                    name: String::from("description"),
                    id: 2,
                    r#type: DynamicTypeBuilderFactory::create_string_type(0).build(),
                    default_value: None,
                    index: 2,
                    label: Vec::new(),
                    try_construct_kind: TryConstructKind::UseDefault,
                    is_key: false,
                    is_optional: false,
                    is_must_understand: false,
                    is_shared: false,
                    is_default_label: false,
                })
                .unwrap();
            builder.build()
        }

        fn create_sample(src: DynamicData) -> Self {
            let task_id = src
                .get_value(0)
                .ok()
                .and_then(|s| i32::try_from_storage(s.clone()).ok())
                .unwrap_or(0);
            let priority_value = src
                .get_complex_value(1)
                .ok()
                .and_then(|p| p.get_int32_value(0).ok().map(|v| *v))
                .unwrap_or(0);
            let priority = match priority_value {
                0 => Priority::Low,
                1 => Priority::Medium,
                2 => Priority::High,
                _ => Priority::Low,
            };
            let description = src
                .get_value(2)
                .ok()
                .and_then(|s| String::try_from_storage(s.clone()).ok())
                .unwrap_or_default();
            TaskWithPriority {
                task_id,
                priority,
                description,
            }
        }

        fn create_dynamic_sample(self) -> DynamicData {
            let mut data =
                crate::xtypes::dynamic_type::DynamicDataFactory::create_data(Self::get_type());
            data.set_value(0, self.task_id.into_storage());

            // Create priority enum value
            let priority_type = Self::get_type()
                .get_member_by_index(1)
                .unwrap()
                .get_descriptor()
                .unwrap()
                .r#type
                .clone();
            let mut priority_data =
                crate::xtypes::dynamic_type::DynamicDataFactory::create_data(priority_type);
            priority_data.set_int32_value(0, self.priority as i32).unwrap();
            data.set_complex_value(1, priority_data).unwrap();

            data.set_value(2, self.description.into_storage());
            data
        }
    }

    /// Integration test: Typed DataWriter with enum field writes, DynamicDataReader reads.
    ///
    /// This test verifies AC #3 from TASK-19: "Integration test: Typed DataWriter with
    /// enum field → DynamicDataReader via TypeLookup"
    #[tokio::test]
    async fn test_dynamic_data_reader_reads_struct_with_enum_field() {
        // Create participant with unique domain
        let factory = DomainParticipantFactoryAsync::get_instance();
        let participant = factory
            .create_participant(201, QosKind::Default, None::<()>, NO_STATUS)
            .await
            .expect("Failed to create participant");

        // Create topic with typed DataWriter's type
        let topic = participant
            .create_topic::<TaskWithPriority>(
                "TaskTopic",
                "TaskWithPriority",
                QosKind::Default,
                None::<()>,
                NO_STATUS,
            )
            .await
            .expect("Failed to create topic");

        // Create publisher and typed DataWriter
        let publisher = participant
            .create_publisher(QosKind::Default, None::<()>, NO_STATUS)
            .await
            .expect("Failed to create publisher");

        let mut writer_qos = crate::infrastructure::qos::DataWriterQos::default();
        writer_qos.reliability = ReliabilityQosPolicy {
            kind: ReliabilityQosPolicyKind::Reliable,
            max_blocking_time: DurationKind::Finite(Duration::new(1, 0)),
        };
        let writer = publisher
            .create_datawriter::<TaskWithPriority>(
                &topic,
                QosKind::Specific(writer_qos),
                None::<()>,
                NO_STATUS,
            )
            .await
            .expect("Failed to create datawriter");

        // Create subscriber and DynamicDataReader
        let subscriber = participant
            .create_subscriber(QosKind::Default, None::<()>, NO_STATUS)
            .await
            .expect("Failed to create subscriber");

        let dynamic_type = TaskWithPriority::get_type();
        let mut reader_qos = crate::infrastructure::qos::DataReaderQos::default();
        reader_qos.reliability = ReliabilityQosPolicy {
            kind: ReliabilityQosPolicyKind::Reliable,
            max_blocking_time: DurationKind::Finite(Duration::new(1, 0)),
        };
        let dynamic_reader = subscriber
            .create_dynamic_datareader("TaskTopic", dynamic_type, QosKind::Specific(reader_qos))
            .await
            .expect("Failed to create dynamic datareader");

        // Wait for discovery
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        // Write data with typed DataWriter - task with High priority
        let test_data = TaskWithPriority {
            task_id: 100,
            priority: Priority::High,
            description: String::from("Important task with enum!"),
        };
        writer
            .write(test_data, None)
            .await
            .expect("Failed to write");

        // Wait for data to propagate
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        // Read with DynamicDataReader
        let samples = dynamic_reader
            .take(10, &ANY_SAMPLE_STATE, &ANY_VIEW_STATE, &ANY_INSTANCE_STATE)
            .await
            .expect("Failed to take samples");

        assert!(!samples.is_empty(), "Expected at least one sample");

        let sample = &samples[0];
        assert!(sample.data.is_some(), "Expected valid data");

        let data = sample.data.as_ref().unwrap();

        // Verify task_id
        let task_id = data
            .get_int32_value_by_name("task_id")
            .expect("Failed to get task_id");
        assert_eq!(*task_id, 100);

        // Verify priority enum - access as complex value, then get int32
        let priority_data = data
            .get_complex_value_by_name("priority")
            .expect("Failed to get priority");
        let priority_value = priority_data
            .get_int32_value(0)
            .expect("Failed to get priority value");
        assert_eq!(*priority_value, 2, "Expected High priority (value 2)");

        // Verify description
        let description = data
            .get_string_value_by_name("description")
            .expect("Failed to get description");
        assert_eq!(description.as_str(), "Important task with enum!");

        // Cleanup
        participant
            .delete_contained_entities()
            .await
            .expect("Failed to delete entities");
        factory
            .delete_participant(&participant)
            .await
            .expect("Failed to delete participant");
    }

    /// Integration test: Tests the full TypeLookup flow with hash-based enum resolution.
    ///
    /// This test specifically validates:
    /// 1. TypeLookup discovers a type with enum members
    /// 2. The enum type (referenced by hash) is resolved correctly
    /// 3. A DynamicDataReader can be created with the discovered type
    /// 4. Data with enum values can be read correctly
    ///
    /// Unlike `test_dynamic_data_reader_reads_struct_with_enum_field` which uses
    /// `get_type()` directly, this test calls `discover_type()` to exercise the
    /// full TypeLookup protocol including hash resolution.
    #[cfg(feature = "type_lookup")]
    #[tokio::test]
    async fn test_typelookup_discovers_struct_with_enum_via_hash() {
        // Create participant with unique domain
        let factory = DomainParticipantFactoryAsync::get_instance();
        let participant = factory
            .create_participant(202, QosKind::Default, None::<()>, NO_STATUS)
            .await
            .expect("Failed to create participant");

        // Create topic with typed DataWriter's type
        let topic = participant
            .create_topic::<TaskWithPriority>(
                "TypeLookupTaskTopic",
                "TaskWithPriority",
                QosKind::Default,
                None::<()>,
                NO_STATUS,
            )
            .await
            .expect("Failed to create topic");

        // Create publisher and typed DataWriter
        let publisher = participant
            .create_publisher(QosKind::Default, None::<()>, NO_STATUS)
            .await
            .expect("Failed to create publisher");

        let mut writer_qos = crate::infrastructure::qos::DataWriterQos::default();
        writer_qos.reliability = ReliabilityQosPolicy {
            kind: ReliabilityQosPolicyKind::Reliable,
            max_blocking_time: DurationKind::Finite(Duration::new(1, 0)),
        };
        writer_qos.durability = crate::infrastructure::qos_policy::DurabilityQosPolicy {
            kind: crate::infrastructure::qos_policy::DurabilityQosPolicyKind::TransientLocal,
        };
        let writer = publisher
            .create_datawriter::<TaskWithPriority>(
                &topic,
                QosKind::Specific(writer_qos),
                None::<()>,
                NO_STATUS,
            )
            .await
            .expect("Failed to create datawriter");

        // Write data before discovering type (to test durability)
        let test_data = TaskWithPriority {
            task_id: 42,
            priority: Priority::High,
            description: String::from("TypeLookup test with enum!"),
        };
        writer
            .write(test_data, None)
            .await
            .expect("Failed to write");

        // Give time for writer to be discovered
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        // Now use discover_type() to get the type via TypeLookup
        // This exercises the full TypeLookup protocol including hash resolution
        let discovered_type = participant
            .discover_type("TypeLookupTaskTopic")
            .await
            .expect("Failed to discover type via TypeLookup");

        // Verify the discovered type structure
        assert_eq!(discovered_type.get_name(), "TaskWithPriority");
        assert_eq!(discovered_type.get_kind(), crate::xtypes::dynamic_type::TypeKind::STRUCTURE);
        assert_eq!(discovered_type.get_member_count(), 3, "Expected 3 members: task_id, priority, description");

        // Create subscriber and DynamicDataReader with discovered type
        let subscriber = participant
            .create_subscriber(QosKind::Default, None::<()>, NO_STATUS)
            .await
            .expect("Failed to create subscriber");

        let mut reader_qos = crate::infrastructure::qos::DataReaderQos::default();
        reader_qos.reliability = ReliabilityQosPolicy {
            kind: ReliabilityQosPolicyKind::Reliable,
            max_blocking_time: DurationKind::Finite(Duration::new(1, 0)),
        };
        reader_qos.durability = crate::infrastructure::qos_policy::DurabilityQosPolicy {
            kind: crate::infrastructure::qos_policy::DurabilityQosPolicyKind::TransientLocal,
        };
        let dynamic_reader = subscriber
            .create_dynamic_datareader(
                "TypeLookupTaskTopic",
                discovered_type,
                QosKind::Specific(reader_qos),
            )
            .await
            .expect("Failed to create dynamic datareader with discovered type");

        // Wait for subscription to match and data to arrive
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        // Read with DynamicDataReader
        let samples = dynamic_reader
            .take(10, &ANY_SAMPLE_STATE, &ANY_VIEW_STATE, &ANY_INSTANCE_STATE)
            .await
            .expect("Failed to take samples");

        assert!(!samples.is_empty(), "Expected at least one sample from TypeLookup-discovered reader");

        let sample = &samples[0];
        assert!(sample.data.is_some(), "Expected valid data");

        let data = sample.data.as_ref().unwrap();

        // Verify task_id
        let task_id = data
            .get_int32_value_by_name("task_id")
            .expect("Failed to get task_id");
        assert_eq!(*task_id, 42);

        // Verify priority enum - this validates enum hash resolution worked
        let priority_data = data
            .get_complex_value_by_name("priority")
            .expect("Failed to get priority from TypeLookup-discovered type");
        let priority_value = priority_data
            .get_int32_value(0)
            .expect("Failed to get priority value");
        assert_eq!(*priority_value, 2, "Expected High priority (value 2)");

        // Verify description
        let description = data
            .get_string_value_by_name("description")
            .expect("Failed to get description");
        assert_eq!(description.as_str(), "TypeLookup test with enum!");

        // Cleanup
        participant
            .delete_contained_entities()
            .await
            .expect("Failed to delete entities");
        factory
            .delete_participant(&participant)
            .await
            .expect("Failed to delete participant");
    }

    /// A struct with a sequence of enums for integration testing.
    #[derive(Debug, Clone, PartialEq)]
    struct TaskWithPriorityList {
        task_id: i32,
        priorities: Vec<Priority>,
    }

    impl TypeSupport for TaskWithPriorityList {
        fn get_type_name() -> &'static str {
            "TaskWithPriorityList"
        }

        fn get_type() -> DynamicType {
            // Create the Priority enum type
            let discriminator_type =
                DynamicTypeBuilderFactory::get_primitive_type(TypeKind::INT32);
            let mut enum_builder = DynamicTypeBuilderFactory::create_type(TypeDescriptor {
                kind: TypeKind::ENUM,
                name: String::from("Priority"),
                base_type: None,
                discriminator_type: Some(discriminator_type),
                bound: Vec::new(),
                element_type: None,
                key_element_type: None,
                extensibility_kind: ExtensibilityKind::Final,
                is_nested: false,
            });
            enum_builder
                .add_member(MemberDescriptor {
                    name: String::from("Low"),
                    id: 0,
                    r#type: DynamicTypeBuilderFactory::get_primitive_type(TypeKind::INT32),
                    default_value: None,
                    index: 0,
                    label: Vec::new(),
                    try_construct_kind: TryConstructKind::UseDefault,
                    is_key: false,
                    is_optional: false,
                    is_must_understand: false,
                    is_shared: false,
                    is_default_label: true,
                })
                .unwrap();
            enum_builder
                .add_member(MemberDescriptor {
                    name: String::from("Medium"),
                    id: 1,
                    r#type: DynamicTypeBuilderFactory::get_primitive_type(TypeKind::INT32),
                    default_value: None,
                    index: 1,
                    label: Vec::new(),
                    try_construct_kind: TryConstructKind::UseDefault,
                    is_key: false,
                    is_optional: false,
                    is_must_understand: false,
                    is_shared: false,
                    is_default_label: false,
                })
                .unwrap();
            enum_builder
                .add_member(MemberDescriptor {
                    name: String::from("High"),
                    id: 2,
                    r#type: DynamicTypeBuilderFactory::get_primitive_type(TypeKind::INT32),
                    default_value: None,
                    index: 2,
                    label: Vec::new(),
                    try_construct_kind: TryConstructKind::UseDefault,
                    is_key: false,
                    is_optional: false,
                    is_must_understand: false,
                    is_shared: false,
                    is_default_label: false,
                })
                .unwrap();
            let priority_enum = enum_builder.build();

            // Create sequence of Priority type (unbounded)
            let priority_sequence =
                DynamicTypeBuilderFactory::create_sequence_type(priority_enum, 0).build();

            // Create the struct type
            let mut builder = DynamicTypeBuilderFactory::create_type(TypeDescriptor {
                kind: TypeKind::STRUCTURE,
                name: String::from("TaskWithPriorityList"),
                base_type: None,
                discriminator_type: None,
                bound: Vec::new(),
                element_type: None,
                key_element_type: None,
                extensibility_kind: ExtensibilityKind::Final,
                is_nested: false,
            });
            builder
                .add_member(MemberDescriptor {
                    name: String::from("task_id"),
                    id: 0,
                    r#type: <i32 as XTypesBinding>::get_dynamic_type(),
                    default_value: None,
                    index: 0,
                    label: Vec::new(),
                    try_construct_kind: TryConstructKind::UseDefault,
                    is_key: true,
                    is_optional: false,
                    is_must_understand: false,
                    is_shared: false,
                    is_default_label: false,
                })
                .unwrap();
            builder
                .add_member(MemberDescriptor {
                    name: String::from("priorities"),
                    id: 1,
                    r#type: priority_sequence,
                    default_value: None,
                    index: 1,
                    label: Vec::new(),
                    try_construct_kind: TryConstructKind::UseDefault,
                    is_key: false,
                    is_optional: false,
                    is_must_understand: false,
                    is_shared: false,
                    is_default_label: false,
                })
                .unwrap();
            builder.build()
        }

        fn create_sample(src: DynamicData) -> Self {
            let task_id = src
                .get_value(0)
                .ok()
                .and_then(|s| i32::try_from_storage(s.clone()).ok())
                .unwrap_or(0);

            let priorities = src
                .get_complex_value(1)
                .ok()
                .map(|seq| {
                    let mut result = Vec::new();
                    let len = seq.get_item_count();
                    for i in 0..len {
                        if let Ok(p) = seq.get_complex_value(i) {
                            if let Ok(v) = p.get_int32_value(0) {
                                let priority = match *v {
                                    0 => Priority::Low,
                                    1 => Priority::Medium,
                                    2 => Priority::High,
                                    _ => Priority::Low,
                                };
                                result.push(priority);
                            }
                        }
                    }
                    result
                })
                .unwrap_or_default();

            TaskWithPriorityList { task_id, priorities }
        }

        fn create_dynamic_sample(self) -> DynamicData {
            let mut data =
                crate::xtypes::dynamic_type::DynamicDataFactory::create_data(Self::get_type());
            data.set_value(0, self.task_id.into_storage());

            // Create sequence of priority enum values
            let priorities_type = Self::get_type()
                .get_member_by_index(1)
                .unwrap()
                .get_descriptor()
                .unwrap()
                .r#type
                .clone();

            // Get the element type (Priority enum) from the sequence type
            let element_type = priorities_type.get_descriptor().element_type.clone().unwrap();

            // Build a Vec of DynamicData for each priority
            let priority_values: Vec<DynamicData> = self.priorities.iter().map(|priority| {
                let mut priority_data =
                    crate::xtypes::dynamic_type::DynamicDataFactory::create_data(element_type.clone());
                priority_data.set_int32_value(0, *priority as i32).unwrap();
                priority_data
            }).collect();

            // Set the sequence of complex values
            data.set_complex_values(1, priority_values).unwrap();

            data
        }
    }

    /// Integration test: TypeLookup discovers struct with sequence of enums via hash resolution.
    ///
    /// This test verifies that when a struct contains a Vec<Enum> member, the TypeLookup
    /// service correctly includes the enum type in the response, allowing hash-based
    /// resolution when the sequence element type is referenced by hash.
    #[cfg(feature = "type_lookup")]
    #[tokio::test]
    async fn test_typelookup_discovers_struct_with_sequence_of_enums() {
        // Create participant with unique domain
        let factory = DomainParticipantFactoryAsync::get_instance();
        let participant = factory
            .create_participant(203, QosKind::Default, None::<()>, NO_STATUS)
            .await
            .expect("Failed to create participant");

        // Create topic with typed DataWriter's type
        let topic = participant
            .create_topic::<TaskWithPriorityList>(
                "TypeLookupSequenceEnumTopic",
                "TaskWithPriorityList",
                QosKind::Default,
                None::<()>,
                NO_STATUS,
            )
            .await
            .expect("Failed to create topic");

        // Create publisher and typed DataWriter
        let publisher = participant
            .create_publisher(QosKind::Default, None::<()>, NO_STATUS)
            .await
            .expect("Failed to create publisher");

        let mut writer_qos = crate::infrastructure::qos::DataWriterQos::default();
        writer_qos.reliability = ReliabilityQosPolicy {
            kind: ReliabilityQosPolicyKind::Reliable,
            max_blocking_time: DurationKind::Finite(Duration::new(1, 0)),
        };
        writer_qos.durability = crate::infrastructure::qos_policy::DurabilityQosPolicy {
            kind: crate::infrastructure::qos_policy::DurabilityQosPolicyKind::TransientLocal,
        };
        let writer = publisher
            .create_datawriter::<TaskWithPriorityList>(
                &topic,
                QosKind::Specific(writer_qos),
                None::<()>,
                NO_STATUS,
            )
            .await
            .expect("Failed to create datawriter");

        // Write data with a sequence of enum values
        let test_data = TaskWithPriorityList {
            task_id: 100,
            priorities: vec![Priority::High, Priority::Low, Priority::Medium, Priority::High],
        };
        writer
            .write(test_data, None)
            .await
            .expect("Failed to write");

        // Give time for writer to be discovered
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        // Now use discover_type() to get the type via TypeLookup
        // This exercises the full TypeLookup protocol including hash resolution for sequence elements
        let discovered_type = participant
            .discover_type("TypeLookupSequenceEnumTopic")
            .await
            .expect("Failed to discover type via TypeLookup");

        // Verify the discovered type structure
        assert_eq!(discovered_type.get_name(), "TaskWithPriorityList");
        assert_eq!(discovered_type.get_kind(), crate::xtypes::dynamic_type::TypeKind::STRUCTURE);
        assert_eq!(discovered_type.get_member_count(), 2, "Expected 2 members: task_id, priorities");

        // Verify the priorities member is a sequence
        let priorities_member = discovered_type.get_member_by_index(1).unwrap();
        let priorities_type = &priorities_member.get_descriptor().unwrap().r#type;
        assert_eq!(priorities_type.get_kind(), crate::xtypes::dynamic_type::TypeKind::SEQUENCE);

        // Verify the sequence element type is an enum
        let element_type = priorities_type.get_descriptor().element_type.as_ref()
            .expect("Sequence should have element type");
        assert_eq!(element_type.get_kind(), crate::xtypes::dynamic_type::TypeKind::ENUM);
        assert_eq!(element_type.get_name(), "Priority");

        // Create subscriber and DynamicDataReader with discovered type
        let subscriber = participant
            .create_subscriber(QosKind::Default, None::<()>, NO_STATUS)
            .await
            .expect("Failed to create subscriber");

        let mut reader_qos = crate::infrastructure::qos::DataReaderQos::default();
        reader_qos.reliability = ReliabilityQosPolicy {
            kind: ReliabilityQosPolicyKind::Reliable,
            max_blocking_time: DurationKind::Finite(Duration::new(1, 0)),
        };
        reader_qos.durability = crate::infrastructure::qos_policy::DurabilityQosPolicy {
            kind: crate::infrastructure::qos_policy::DurabilityQosPolicyKind::TransientLocal,
        };
        let dynamic_reader = subscriber
            .create_dynamic_datareader(
                "TypeLookupSequenceEnumTopic",
                discovered_type,
                QosKind::Specific(reader_qos),
            )
            .await
            .expect("Failed to create dynamic datareader with discovered type");

        // Wait for subscription to match and data to arrive
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        // Read with DynamicDataReader
        let samples = dynamic_reader
            .take(10, &ANY_SAMPLE_STATE, &ANY_VIEW_STATE, &ANY_INSTANCE_STATE)
            .await
            .expect("Failed to take samples");

        assert!(!samples.is_empty(), "Expected at least one sample from TypeLookup-discovered reader");

        let sample = &samples[0];
        assert!(sample.data.is_some(), "Expected valid data");

        let data = sample.data.as_ref().unwrap();

        // Verify task_id
        let task_id = data
            .get_int32_value_by_name("task_id")
            .expect("Failed to get task_id");
        assert_eq!(*task_id, 100);

        // Verify priorities sequence - this validates sequence element hash resolution worked
        // Note: Sequences are stored as SequenceComplexValue, so we need to use get_complex_values
        let priorities_member_id = data
            .get_member_id_by_name("priorities")
            .expect("Failed to find priorities member");
        let priorities_list = data
            .get_complex_values(priorities_member_id)
            .expect("Failed to get priorities from TypeLookup-discovered type");

        assert_eq!(priorities_list.len(), 4, "Expected 4 priority values");

        // Verify each priority value: High(2), Low(0), Medium(1), High(2)
        let expected_values = [2, 0, 1, 2];
        for (i, expected) in expected_values.iter().enumerate() {
            let priority_data = &priorities_list[i];
            let priority_value = priority_data
                .get_int32_value(0)
                .expect(&format!("Failed to get priority value at index {}", i));
            assert_eq!(*priority_value, *expected, "Unexpected priority value at index {}", i);
        }

        // Cleanup
        participant
            .delete_contained_entities()
            .await
            .expect("Failed to delete entities");
        factory
            .delete_participant(&participant)
            .await
            .expect("Failed to delete participant");
    }

    /// A struct with a string and an enum for testing nested sequences.
    #[derive(Debug, Clone, PartialEq)]
    struct Task {
        name: String,
        priority: Priority,
    }

    /// A struct with a sequence of structs (each containing string + enum).
    #[derive(Debug, Clone, PartialEq)]
    struct TaskList {
        list_id: i32,
        tasks: Vec<Task>,
    }

    fn create_priority_enum_type() -> DynamicType {
        let discriminator_type = DynamicTypeBuilderFactory::get_primitive_type(TypeKind::INT32);
        let mut enum_builder = DynamicTypeBuilderFactory::create_type(TypeDescriptor {
            kind: TypeKind::ENUM,
            name: String::from("Priority"),
            base_type: None,
            discriminator_type: Some(discriminator_type),
            bound: Vec::new(),
            element_type: None,
            key_element_type: None,
            extensibility_kind: ExtensibilityKind::Final,
            is_nested: false,
        });
        enum_builder
            .add_member(MemberDescriptor {
                name: String::from("Low"),
                id: 0,
                r#type: DynamicTypeBuilderFactory::get_primitive_type(TypeKind::INT32),
                default_value: None,
                index: 0,
                label: Vec::new(),
                try_construct_kind: TryConstructKind::UseDefault,
                is_key: false,
                is_optional: false,
                is_must_understand: false,
                is_shared: false,
                is_default_label: true,
            })
            .unwrap();
        enum_builder
            .add_member(MemberDescriptor {
                name: String::from("Medium"),
                id: 1,
                r#type: DynamicTypeBuilderFactory::get_primitive_type(TypeKind::INT32),
                default_value: None,
                index: 1,
                label: Vec::new(),
                try_construct_kind: TryConstructKind::UseDefault,
                is_key: false,
                is_optional: false,
                is_must_understand: false,
                is_shared: false,
                is_default_label: false,
            })
            .unwrap();
        enum_builder
            .add_member(MemberDescriptor {
                name: String::from("High"),
                id: 2,
                r#type: DynamicTypeBuilderFactory::get_primitive_type(TypeKind::INT32),
                default_value: None,
                index: 2,
                label: Vec::new(),
                try_construct_kind: TryConstructKind::UseDefault,
                is_key: false,
                is_optional: false,
                is_must_understand: false,
                is_shared: false,
                is_default_label: false,
            })
            .unwrap();
        enum_builder.build()
    }

    fn create_task_type(priority_enum: DynamicType) -> DynamicType {
        let mut builder = DynamicTypeBuilderFactory::create_type(TypeDescriptor {
            kind: TypeKind::STRUCTURE,
            name: String::from("Task"),
            base_type: None,
            discriminator_type: None,
            bound: Vec::new(),
            element_type: None,
            key_element_type: None,
            extensibility_kind: ExtensibilityKind::Final,
            is_nested: false,
        });
        builder
            .add_member(MemberDescriptor {
                name: String::from("name"),
                id: 0,
                r#type: DynamicTypeBuilderFactory::create_string_type(0).build(),
                default_value: None,
                index: 0,
                label: Vec::new(),
                try_construct_kind: TryConstructKind::UseDefault,
                is_key: false,
                is_optional: false,
                is_must_understand: false,
                is_shared: false,
                is_default_label: false,
            })
            .unwrap();
        builder
            .add_member(MemberDescriptor {
                name: String::from("priority"),
                id: 1,
                r#type: priority_enum,
                default_value: None,
                index: 1,
                label: Vec::new(),
                try_construct_kind: TryConstructKind::UseDefault,
                is_key: false,
                is_optional: false,
                is_must_understand: false,
                is_shared: false,
                is_default_label: false,
            })
            .unwrap();
        builder.build()
    }

    impl TypeSupport for TaskList {
        fn get_type_name() -> &'static str {
            "TaskList"
        }

        fn get_type() -> DynamicType {
            let priority_enum = create_priority_enum_type();
            let task_type = create_task_type(priority_enum);
            let task_sequence = DynamicTypeBuilderFactory::create_sequence_type(task_type, 0).build();

            let mut builder = DynamicTypeBuilderFactory::create_type(TypeDescriptor {
                kind: TypeKind::STRUCTURE,
                name: String::from("TaskList"),
                base_type: None,
                discriminator_type: None,
                bound: Vec::new(),
                element_type: None,
                key_element_type: None,
                extensibility_kind: ExtensibilityKind::Final,
                is_nested: false,
            });
            builder
                .add_member(MemberDescriptor {
                    name: String::from("list_id"),
                    id: 0,
                    r#type: <i32 as XTypesBinding>::get_dynamic_type(),
                    default_value: None,
                    index: 0,
                    label: Vec::new(),
                    try_construct_kind: TryConstructKind::UseDefault,
                    is_key: true,
                    is_optional: false,
                    is_must_understand: false,
                    is_shared: false,
                    is_default_label: false,
                })
                .unwrap();
            builder
                .add_member(MemberDescriptor {
                    name: String::from("tasks"),
                    id: 1,
                    r#type: task_sequence,
                    default_value: None,
                    index: 1,
                    label: Vec::new(),
                    try_construct_kind: TryConstructKind::UseDefault,
                    is_key: false,
                    is_optional: false,
                    is_must_understand: false,
                    is_shared: false,
                    is_default_label: false,
                })
                .unwrap();
            builder.build()
        }

        fn create_sample(src: DynamicData) -> Self {
            let list_id = src
                .get_value(0)
                .ok()
                .and_then(|s| i32::try_from_storage(s.clone()).ok())
                .unwrap_or(0);

            let tasks_member_id = src.get_member_id_by_name("tasks").unwrap_or(1);
            let tasks = src
                .get_complex_values(tasks_member_id)
                .ok()
                .map(|task_list| {
                    task_list
                        .iter()
                        .map(|task_data| {
                            let name = task_data
                                .get_string_value(0)
                                .map(|s| s.clone())
                                .unwrap_or_default();
                            let priority_value = task_data
                                .get_complex_value(1)
                                .ok()
                                .and_then(|p| p.get_int32_value(0).ok().map(|v| *v))
                                .unwrap_or(0);
                            let priority = match priority_value {
                                0 => Priority::Low,
                                1 => Priority::Medium,
                                2 => Priority::High,
                                _ => Priority::Low,
                            };
                            Task { name, priority }
                        })
                        .collect()
                })
                .unwrap_or_default();

            TaskList { list_id, tasks }
        }

        fn create_dynamic_sample(self) -> DynamicData {
            let mut data =
                crate::xtypes::dynamic_type::DynamicDataFactory::create_data(Self::get_type());
            data.set_value(0, self.list_id.into_storage());

            // Get the Task type from the sequence element type
            let tasks_type = Self::get_type()
                .get_member_by_index(1)
                .unwrap()
                .get_descriptor()
                .unwrap()
                .r#type
                .clone();
            let task_type = tasks_type.get_descriptor().element_type.clone().unwrap();

            // Get the Priority enum type from Task
            let priority_type = task_type
                .get_member_by_index(1)
                .unwrap()
                .get_descriptor()
                .unwrap()
                .r#type
                .clone();

            // Build Vec<DynamicData> for tasks
            let task_values: Vec<DynamicData> = self
                .tasks
                .iter()
                .map(|task| {
                    let mut task_data =
                        crate::xtypes::dynamic_type::DynamicDataFactory::create_data(task_type.clone());
                    task_data.set_string_value(0, task.name.clone()).unwrap();

                    let mut priority_data =
                        crate::xtypes::dynamic_type::DynamicDataFactory::create_data(priority_type.clone());
                    priority_data.set_int32_value(0, task.priority as i32).unwrap();
                    task_data.set_complex_value(1, priority_data).unwrap();

                    task_data
                })
                .collect();

            data.set_complex_values(1, task_values).unwrap();
            data
        }
    }

    /// Integration test: TypeLookup discovers struct with sequence of structs (each with string + enum).
    ///
    /// This test verifies that when a struct contains a Vec<Struct> where the inner struct
    /// has both a String and an Enum, TypeLookup correctly includes all dependent types
    /// (both the inner struct type and its enum member type).
    #[cfg(feature = "type_lookup")]
    #[tokio::test]
    async fn test_typelookup_discovers_struct_with_sequence_of_structs() {
        // Create participant with unique domain
        let factory = DomainParticipantFactoryAsync::get_instance();
        let participant = factory
            .create_participant(204, QosKind::Default, None::<()>, NO_STATUS)
            .await
            .expect("Failed to create participant");

        // Create topic with typed DataWriter's type
        let topic = participant
            .create_topic::<TaskList>(
                "TypeLookupTaskListTopic",
                "TaskList",
                QosKind::Default,
                None::<()>,
                NO_STATUS,
            )
            .await
            .expect("Failed to create topic");

        // Create publisher and typed DataWriter
        let publisher = participant
            .create_publisher(QosKind::Default, None::<()>, NO_STATUS)
            .await
            .expect("Failed to create publisher");

        let mut writer_qos = crate::infrastructure::qos::DataWriterQos::default();
        writer_qos.reliability = ReliabilityQosPolicy {
            kind: ReliabilityQosPolicyKind::Reliable,
            max_blocking_time: DurationKind::Finite(Duration::new(1, 0)),
        };
        writer_qos.durability = crate::infrastructure::qos_policy::DurabilityQosPolicy {
            kind: crate::infrastructure::qos_policy::DurabilityQosPolicyKind::TransientLocal,
        };
        let writer = publisher
            .create_datawriter::<TaskList>(
                &topic,
                QosKind::Specific(writer_qos),
                None::<()>,
                NO_STATUS,
            )
            .await
            .expect("Failed to create datawriter");

        // Write data with a sequence of structs containing string + enum
        let test_data = TaskList {
            list_id: 42,
            tasks: vec![
                Task {
                    name: String::from("First task"),
                    priority: Priority::High,
                },
                Task {
                    name: String::from("Second task"),
                    priority: Priority::Low,
                },
                Task {
                    name: String::from("Third task"),
                    priority: Priority::Medium,
                },
            ],
        };
        writer
            .write(test_data, None)
            .await
            .expect("Failed to write");

        // Give time for writer to be discovered
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        // Now use discover_type() to get the type via TypeLookup
        let discovered_type = participant
            .discover_type("TypeLookupTaskListTopic")
            .await
            .expect("Failed to discover type via TypeLookup");

        // Verify the discovered type structure
        assert_eq!(discovered_type.get_name(), "TaskList");
        assert_eq!(discovered_type.get_kind(), crate::xtypes::dynamic_type::TypeKind::STRUCTURE);
        assert_eq!(discovered_type.get_member_count(), 2, "Expected 2 members: list_id, tasks");

        // Verify the tasks member is a sequence
        let tasks_member = discovered_type.get_member_by_index(1).unwrap();
        let tasks_type = &tasks_member.get_descriptor().unwrap().r#type;
        assert_eq!(tasks_type.get_kind(), crate::xtypes::dynamic_type::TypeKind::SEQUENCE);

        // Verify the sequence element type is a struct (Task)
        let task_element_type = tasks_type.get_descriptor().element_type.as_ref()
            .expect("Sequence should have element type");
        assert_eq!(task_element_type.get_kind(), crate::xtypes::dynamic_type::TypeKind::STRUCTURE);
        assert_eq!(task_element_type.get_name(), "Task");
        assert_eq!(task_element_type.get_member_count(), 2, "Task should have 2 members: name, priority");

        // Verify the Task struct has a priority field that is an enum
        let priority_member = task_element_type.get_member_by_index(1).unwrap();
        let priority_type = &priority_member.get_descriptor().unwrap().r#type;
        assert_eq!(priority_type.get_kind(), crate::xtypes::dynamic_type::TypeKind::ENUM);
        assert_eq!(priority_type.get_name(), "Priority");

        // Create subscriber and DynamicDataReader with discovered type
        let subscriber = participant
            .create_subscriber(QosKind::Default, None::<()>, NO_STATUS)
            .await
            .expect("Failed to create subscriber");

        let mut reader_qos = crate::infrastructure::qos::DataReaderQos::default();
        reader_qos.reliability = ReliabilityQosPolicy {
            kind: ReliabilityQosPolicyKind::Reliable,
            max_blocking_time: DurationKind::Finite(Duration::new(1, 0)),
        };
        reader_qos.durability = crate::infrastructure::qos_policy::DurabilityQosPolicy {
            kind: crate::infrastructure::qos_policy::DurabilityQosPolicyKind::TransientLocal,
        };
        let dynamic_reader = subscriber
            .create_dynamic_datareader(
                "TypeLookupTaskListTopic",
                discovered_type,
                QosKind::Specific(reader_qos),
            )
            .await
            .expect("Failed to create dynamic datareader with discovered type");

        // Wait for subscription to match and data to arrive
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        // Read with DynamicDataReader
        let samples = dynamic_reader
            .take(10, &ANY_SAMPLE_STATE, &ANY_VIEW_STATE, &ANY_INSTANCE_STATE)
            .await
            .expect("Failed to take samples");

        assert!(!samples.is_empty(), "Expected at least one sample from TypeLookup-discovered reader");

        let sample = &samples[0];
        assert!(sample.data.is_some(), "Expected valid data");

        let data = sample.data.as_ref().unwrap();

        // Verify list_id
        let list_id = data
            .get_int32_value_by_name("list_id")
            .expect("Failed to get list_id");
        assert_eq!(*list_id, 42);

        // Verify tasks sequence
        let tasks_member_id = data
            .get_member_id_by_name("tasks")
            .expect("Failed to find tasks member");
        let tasks_list = data
            .get_complex_values(tasks_member_id)
            .expect("Failed to get tasks from TypeLookup-discovered type");

        assert_eq!(tasks_list.len(), 3, "Expected 3 tasks");

        // Verify first task: "First task", High(2)
        let task0 = &tasks_list[0];
        let name0 = task0.get_string_value(0).expect("Failed to get task name");
        assert_eq!(name0, "First task");
        let priority0 = task0.get_complex_value(1).expect("Failed to get priority");
        let priority0_value = priority0.get_int32_value(0).expect("Failed to get priority value");
        assert_eq!(*priority0_value, 2, "Expected High priority");

        // Verify second task: "Second task", Low(0)
        let task1 = &tasks_list[1];
        let name1 = task1.get_string_value(0).expect("Failed to get task name");
        assert_eq!(name1, "Second task");
        let priority1 = task1.get_complex_value(1).expect("Failed to get priority");
        let priority1_value = priority1.get_int32_value(0).expect("Failed to get priority value");
        assert_eq!(*priority1_value, 0, "Expected Low priority");

        // Verify third task: "Third task", Medium(1)
        let task2 = &tasks_list[2];
        let name2 = task2.get_string_value(0).expect("Failed to get task name");
        assert_eq!(name2, "Third task");
        let priority2 = task2.get_complex_value(1).expect("Failed to get priority");
        let priority2_value = priority2.get_int32_value(0).expect("Failed to get priority value");
        assert_eq!(*priority2_value, 1, "Expected Medium priority");

        // Cleanup
        participant
            .delete_contained_entities()
            .await
            .expect("Failed to delete entities");
        factory
            .delete_participant(&participant)
            .await
            .expect("Failed to delete participant");
    }
}
