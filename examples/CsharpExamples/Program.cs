﻿using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Pulsar.Client.Api;

namespace CsharpExamples
{
    internal class Program
    {
        private static async Task Main(string[] args)
        {
            var loggerFactory =
               LoggerFactory.Create(builder =>
                   builder
                       .SetMinimumLevel(LogLevel.Information)
                       .AddConsole()
               );
            PulsarClient.Logger = loggerFactory.CreateLogger("PulsarLogger");

            // await Simple.RunSimple();
            // await TlsAuthentication.RunTlsAuthentication();
            // await CustomProps.RunCustomProps();
            // await ReaderApi.RunReader();
            // await RealWorld.RunRealWorld(PulsarClient.Logger);
            // await SchemaExample.RunSchema();
            // await TransactionExample.RunTransaction();
            //await Oauth2.RunOauth();
            await Telemetry.RunTelemetry();

            Console.WriteLine("Example ended. Press any key to exit");
            Console.ReadKey();
        }
    }
}