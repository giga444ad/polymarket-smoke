import { Module } from '@nestjs/common';
import { APP_GUARD } from '@nestjs/core';
import { ConfigModule, ConfigService } from '@nestjs/config';
import { JwtModule } from '@nestjs/jwt';
import { PassportModule } from '@nestjs/passport';
import { TypeOrmModule } from '@nestjs/typeorm';
import { User } from './user.entity';
import { AuthService } from './auth.service';
import { AuthController } from './auth.controller';
import { JwtStrategy } from './strategies/jwt.strategy';
import { JwtAuthGuard } from './guards/jwt-auth.guard';
import { RemoteJwtGuard } from './guards/remote-jwt.guard';
import { RolesGuard } from './guards/roles.guard';

@Module({})
export class AuthModule {
  static register() {
    const isMain = process.env.IS_MAIN === 'true';

    return {
      module: AuthModule,
      imports: [
        TypeOrmModule.forFeature([User]),
        ...(isMain
          ? [
              PassportModule,
              JwtModule.registerAsync({
                imports: [ConfigModule],
                inject: [ConfigService],
                useFactory: (config: ConfigService) => ({
                  secret: config.get<string>('JWT_SECRET'),
                  signOptions: { algorithm: 'HS256' },
                }),
              }),
            ]
          : []),
      ],
      controllers: isMain ? [AuthController] : [],
      providers: [
        ...(isMain ? [AuthService, JwtStrategy] : []),
        {
          provide: APP_GUARD,
          useClass: isMain ? JwtAuthGuard : RemoteJwtGuard,
        },
        {
          provide: APP_GUARD,
          useClass: RolesGuard,
        },
      ],
      exports: isMain ? [AuthService] : [],
    };
  }
}
