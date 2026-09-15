import { Injectable, UnauthorizedException } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { JwtService } from '@nestjs/jwt';
import { InjectRepository } from '@nestjs/typeorm';
import * as bcrypt from 'bcrypt';
import { Repository } from 'typeorm';
import { User } from './user.entity';

@Injectable()
export class AuthService {
  constructor(
    @InjectRepository(User) private readonly users: Repository<User>,
    private readonly jwt: JwtService,
    private readonly config: ConfigService,
  ) {}

  async login(username: string, password: string) {
    const invalid = () => new UnauthorizedException('Неверный логин или пароль');

    const user = await this.users.findOne({ where: { username } });
    if (!user || !user.isActive) throw invalid();

    const ok = await bcrypt.compare(password, user.passwordHash);
    if (!ok) throw invalid();

    const payload = { sub: user.id, username: user.username, role: user.role };
    const expiresIn = this.config.get<string>('JWT_EXPIRES_IN', '12h') as unknown as number;
    const token = await this.jwt.signAsync(payload, { expiresIn });
    return { accessToken: token, tokenType: 'Bearer', role: user.role, username: user.username };
  }
}
