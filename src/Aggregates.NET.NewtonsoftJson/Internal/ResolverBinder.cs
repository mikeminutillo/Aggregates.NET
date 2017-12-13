﻿using Aggregates.Contracts;
using Newtonsoft.Json.Serialization;
using System.Reflection;
using System;
using Newtonsoft.Json;

namespace Aggregates.Internal
{
    class EventContractResolver : DefaultContractResolver
    {
        private readonly IEventMapper _mapper;
        private readonly IEventFactory _factory;

        public EventContractResolver(IEventMapper mapper, IEventFactory factory)
        {
            _mapper = mapper;
            _factory = factory;
        }

        // https://github.com/danielwertheim/jsonnet-privatesetterscontractresolvers/blob/master/src/JsonNet.PrivateSettersContractResolvers/PrivateSettersContractResolvers.cs
        // Need to be able to set private members because snapshots generally are { get; private set; } which won't deserialize properly
        protected override JsonProperty CreateProperty(MemberInfo member, MemberSerialization memberSerialization)
        {
            var jProperty = base.CreateProperty(member, memberSerialization);
            if (jProperty.Writable)
                return jProperty;

            jProperty.Writable = isPropertyWithSetter(member);

            return jProperty;
        }
        private bool isPropertyWithSetter(MemberInfo member)
        {
            var property = member as PropertyInfo;

            return property?.GetSetMethod(true) != null;
        }
        protected override JsonObjectContract CreateObjectContract(Type objectType)
        {
            var mappedTypeFor = objectType;

            if(!mappedTypeFor.IsInterface)
                return base.CreateObjectContract(objectType);
            
            mappedTypeFor = _mapper.GetMappedTypeFor(objectType);

            if (mappedTypeFor == null)
                return base.CreateObjectContract(objectType);

            var objectContract = base.CreateObjectContract(mappedTypeFor);

            objectContract.DefaultCreator = () => _factory.Create(mappedTypeFor);

            return objectContract;
        }
    }

    class EventSerializationBinder : DefaultSerializationBinder
    {
        private readonly IEventMapper _mapper;

        public EventSerializationBinder(IEventMapper mapper)
        {
            _mapper = mapper;
        }

        public override void BindToName(Type serializedType, out string assemblyName, out string typeName)
        {
            var mappedType = serializedType;
            if (!serializedType.IsInterface)
                mappedType = _mapper.GetMappedTypeFor(serializedType) ?? serializedType;

            assemblyName = null;
            typeName = mappedType.AssemblyQualifiedName;
        }
    }
}