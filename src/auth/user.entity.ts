import { Column, CreateDateColumn, Entity, PrimaryGeneratedColumn, UpdateDateColumn } from 'typeorm';
import { Role } from './role.enum';

// Пользователи создаются и назначаются на роли ВРУЧНУЮ в БД (см.
// scripts/create-user.ts) — эндпоинта регистрации нет и не планируется.
@Entity('users')
export class User {
  @PrimaryGeneratedColumn('uuid')
  id: string;

  @Column({ unique: true })
  username: string;

  // Только bcrypt-хэш, plain-пароль никогда не хранится и не логируется.
  @Column({ name: 'password_hash' })
  passwordHash: string;

  @Column({ type: 'enum', enum: Role, default: Role.VIEWER })
  role: Role;

  @Column({ name: 'is_active', default: true })
  isActive: boolean;

  @CreateDateColumn({ name: 'created_at' })
  createdAt: Date;

  @UpdateDateColumn({ name: 'updated_at' })
  updatedAt: Date;
}
