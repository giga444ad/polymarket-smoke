import {
  CanActivate,
  ExecutionContext,
  Injectable,
  Logger,
  ServiceUnavailableException,
  UnauthorizedException,
} from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Reflector } from '@nestjs/core';
import axios from 'axios';
import { IS_PUBLIC_KEY } from '../decorators/public.decorator';
import { RequestUser } from '../decorators/current-user.decorator';

interface CacheEntry {
  user: RequestUser;
  expiresAt: number;
}

// Используется на всех сервисах с IS_MAIN=false (btc/eth/sol/doge — не тот,
// на который навешан публичный домен). Секрета JWT здесь НЕТ ВООБЩЕ — токен
// уходит на проверку в основной сервис (единственный источник правды по
// пользователям/ролям). Чтобы "закрыть ручки градом" запросов не превращалось
// в град запросов К ГЛАВНОМУ сервису, результат кэшируется в памяти на
// AUTH_REMOTE_CACHE_TTL_MS (по токену, не по пользователю — при протухании
// или отзыве токена кэш просто перестанет обновляться при следующем 401).
@Injectable()
export class RemoteJwtGuard implements CanActivate {
  private readonly logger = new Logger(RemoteJwtGuard.name);
  private readonly cache = new Map<string, CacheEntry>();

  constructor(
    private readonly reflector: Reflector,
    private readonly config: ConfigService,
  ) {}

  async canActivate(context: ExecutionContext): Promise<boolean> {
    const isPublic = this.reflector.getAllAndOverride<boolean>(IS_PUBLIC_KEY, [
      context.getHandler(),
      context.getClass(),
    ]);
    if (isPublic) return true;

    const request = context.switchToHttp().getRequest();
    const token = this.extractToken(request);
    if (!token) throw new UnauthorizedException('Отсутствует токен авторизации');

    const cached = this.cache.get(token);
    if (cached && cached.expiresAt > Date.now()) {
      request.user = cached.user;
      return true;
    }

    const mainUrl = this.config.get<string>('MAIN_VALIDATE_URL');
    if (!mainUrl) {
      throw new ServiceUnavailableException(
        'MAIN_VALIDATE_URL не задан — этот сервис не IS_MAIN и не может проверить токен сам.',
      );
    }

    try {
      const response = await axios.get(`${mainUrl.replace(/\/+$/, '')}/api/auth/validate`, {
        headers: { Authorization: `Bearer ${token}` },
        timeout: parseInt(this.config.get<string>('MAIN_VALIDATE_TIMEOUT_MS', '4000'), 10),
      });
      const user = response.data as RequestUser;
      const ttlMs = parseInt(this.config.get<string>('AUTH_REMOTE_CACHE_TTL_MS', '30000'), 10);
      this.cache.set(token, { user, expiresAt: Date.now() + ttlMs });
      request.user = user;
      return true;
    } catch (err: any) {
      this.cache.delete(token);
      if (err?.response?.status === 401) {
        throw new UnauthorizedException('Токен отклонён основным сервисом');
      }
      this.logger.error(`Не удалось проверить токен через ${mainUrl}: ${err?.message}`);
      throw new ServiceUnavailableException('Основной сервис аутентификации недоступен');
    }
  }

  private extractToken(request: any): string | null {
    const header = request.headers?.authorization;
    if (!header || !header.startsWith('Bearer ')) return null;
    return header.slice(7).trim() || null;
  }
}
