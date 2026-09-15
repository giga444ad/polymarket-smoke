import { Injectable, UnauthorizedException } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { PassportStrategy } from '@nestjs/passport';
import { ExtractJwt, Strategy } from 'passport-jwt';
import { RequestUser } from '../decorators/current-user.decorator';

// Используется ТОЛЬКО когда IS_MAIN=true — этот процесс единолично держит
// JWT_SECRET и умеет проверять подпись локально (без сетевого похода).
// На не-IS_MAIN сервисах эта стратегия не регистрируется вообще (см.
// AuthModule) — секрет там просто не нужен и никогда не попадает в их ENV.
@Injectable()
export class JwtStrategy extends PassportStrategy(Strategy, 'jwt-local') {
  constructor(config: ConfigService) {
    const secret = config.get<string>('JWT_SECRET');
    if (!secret || secret.length < 32) {
      throw new Error(
        'JWT_SECRET отсутствует или короче 32 символов. На IS_MAIN=true сервисе это обязательно ' +
          '(сгенерировать: `openssl rand -hex 32`).',
      );
    }
    super({
      jwtFromRequest: ExtractJwt.fromAuthHeaderAsBearerToken(),
      ignoreExpiration: false,
      secretOrKey: secret,
      algorithms: ['HS256'],
    });
  }

  async validate(payload: RequestUser): Promise<RequestUser> {
    if (!payload?.sub || !payload?.role) {
      throw new UnauthorizedException('Некорректный токен');
    }
    return payload;
  }
}
