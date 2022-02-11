﻿using Aggregates.Contracts;
using FakeItEasy;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace Aggregates.Common
{
    public class Configuration : Test
    {
        [Fact]
        public async Task ShouldBuildDefaultConfiguration()
        {
            var container = Fake<IServiceCollection>();
            var config = await Aggregates.Configuration.Build(container, config =>
             {
             }).ConfigureAwait(false);

            config.Setup.Should().BeTrue();
        }


        [Fact]
        public async Task ShouldRequireContainerDefinition()
        {
            var container = Fake<IServiceCollection>();
            var e = await Record.ExceptionAsync(() => Aggregates.Configuration.Build(null, config => { })).ConfigureAwait(false);
            e.Should().BeOfType<ArgumentException>();
        }

        [Fact]
        public async Task ShouldCallRegistrationTasks()
        {
            bool called = false;
            var collection = Fake<IServiceCollection>();
            var provider = Fake<IServiceProvider>();
            await Aggregates.Configuration.Build(collection, config =>
            {
                Configure.RegistrationTasks.Add((container, _) =>
                {
                    called = true;
                    return Task.CompletedTask;
                });
            }).ConfigureAwait(false);

            called.Should().BeTrue();
        }
        [Fact]
        public async Task ShouldNotCallSetupTasks()
        {
            bool called = false;
            var collection = Fake<IServiceCollection>();
            var provider = Fake<IServiceProvider>();
            await Aggregates.Configuration.Build(collection, config =>
            {
                Configure.SetupTasks.Add((container, _) =>
                {
                    called = true;
                    return Task.CompletedTask;
                });
            }).ConfigureAwait(false);

            called.Should().BeFalse();
        }
        [Fact]
        public async Task ShouldNotBeSetupAfterRegistrationException()
        {
            var collection = Fake<IServiceCollection>();
            var provider = Fake<IServiceProvider>();
            var e = await Record.ExceptionAsync(() => Aggregates.Configuration.Build(collection, config =>
            {
                Configure.RegistrationTasks.Add((container, _) =>
                {
                    throw new Exception();
                });
            })).ConfigureAwait(false);

            e.Should().BeOfType<Exception>();
        }
        [Fact]
        public async Task ShouldNotBeSetupAfterSetupException()
        {
            var collection = Fake<IServiceCollection>();
            var provider = Fake<IServiceProvider>();
            var config = await Aggregates.Configuration.Build(collection, config =>
            {
                Configure.SetupTasks.Add((container, _) =>
                {
                    throw new Exception();
                });
            });

            var e = await Record.ExceptionAsync(() => config.Start(provider)).ConfigureAwait(false);

            e.Should().BeOfType<Exception>();
            config.Setup.Should().BeFalse();
        }
        [Fact]
        public async Task ShouldSetOptions()
        {
            var provider = Fake<IServiceCollection>();
            var config = await Aggregates.Configuration.Build(provider, config =>
            {
                config.SetEndpointName("test");
                config.SetSlowAlertThreshold(TimeSpan.FromSeconds(1));
                config.SetExtraStats(true);
                config.SetStreamIdGenerator((type, streamType, bucket, stream, parents) => "test");
                config.SetReadSize(1);
                config.SetCompression(Compression.All);
                config.SetUniqueAddress("test");
                config.SetPassive();
            }).ConfigureAwait(false);

            config.Settings.Endpoint.Should().Be("test");
            config.Settings.SlowAlertThreshold.Should().Be(TimeSpan.FromSeconds(1));
            config.Settings.ExtraStats.Should().BeTrue();
            config.Settings.Generator(null, null, null, null, null).Should().Be("test");
            config.Settings.ReadSize.Should().Be(1);
            config.Settings.Compression.Should().Be(Compression.All);
            config.Settings.UniqueAddress.Should().Be("test");
            config.Settings.Passive.Should().BeTrue();
        }

    }
}
