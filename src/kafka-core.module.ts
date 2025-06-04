import { Module, Global } from '@nestjs/common';
import { ModulesContainer, MetadataScanner, Reflector } from '@nestjs/core';

@Global()
@Module({
  providers: [
    ModulesContainer,
    MetadataScanner,
    Reflector,
  ],
  exports: [
    ModulesContainer,
    MetadataScanner,
    Reflector,
  ],
})
export class KafkaCoreModule {} 