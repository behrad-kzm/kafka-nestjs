import { Module, Global, DynamicModule, Provider } from '@nestjs/common';
import { DiscoveryModule } from '@nestjs/core';
import { KafkaDynamicListenerService } from './services/kafka-dynamic-listener.service';
import {
  SharedKafkaAsyncConfiguration,
} from './interfaces/shared-queue-async-configuration';
import { KAFKA_MODULE_OPTIONS } from './constants/kafka.constants';
import { KafkaProducerService } from './services/kafka-producer.service';
import { KafkaConfig } from 'kafkajs';

@Global()
@Module({})
export class KafkaModule {
  static forProducer(): DynamicModule {
    return {
      module: KafkaModule,
      imports: [DiscoveryModule],
      providers: [
        {
          provide: 'KAFKA_PARTITIONER',
          useValue: undefined,
        },
        KafkaProducerService,
      ],
      exports: [KafkaProducerService],
    };
  }

  static forRoot(options: KafkaConfig): DynamicModule {
    return {
      module: KafkaModule,
      imports: [DiscoveryModule],
      providers: [
        {
          provide: KAFKA_MODULE_OPTIONS,
          useValue: options,
        },
        {
          provide: 'KAFKA_PARTITIONER',
          useValue: undefined,
        },
        KafkaDynamicListenerService,
        KafkaProducerService,
      ],
      exports: [KafkaProducerService],
    };
  }

  static forRootAsync(options: SharedKafkaAsyncConfiguration): DynamicModule {
    const asyncProvider: Provider = {
      provide: KAFKA_MODULE_OPTIONS,
      useFactory: async (...args: any[]) => {
        const config = await options.useFactory!(...args);
        return config;
      },
      inject: options.inject || [],
    };

    return {
      module: KafkaModule,
      imports: [DiscoveryModule],
      providers: [
        asyncProvider,
        {
          provide: 'KAFKA_PARTITIONER',
          useValue: undefined,
        },
        KafkaDynamicListenerService,
        KafkaProducerService,
      ],
      exports: [KafkaProducerService],
    };
  }
}