import { Column, CreateDateColumn, Entity, PrimaryGeneratedColumn, UpdateDateColumn } from 'typeorm';
import { Role } from './role.enum';

// Пользователи создаются и назначаются на роли ВРУЧНУЮ в БД (см.
// scripts/create-user.ts) — эндпоинта регистрации нет и не планируется.
//
// ВАЖНО: все @Column() ниже указывают `type` ЯВНО (а не полагаются на
// TypeORM-автовывод из TS-типа поля через emitDecoratorMetadata). Причина:
// scripts/create-user.ts запускается через `tsx` (esbuild), а esbuild
// emitDecoratorMetadata не реализует — без явного `type` TypeORM не может
// понять тип колонки и падает с ColumnTypeUndefinedError именно в момент
// запуска через tsx (хотя обычная сборка Nest через tsc отработала бы и без
// этого). Явный type работает одинаково в обоих случаях, поэтому он тут
// везде, а не только там, где раньше падало.
@Entity('users')
export class User {
  @PrimaryGeneratedColumn('uuid')
  id: string;

  @Column({ type: 'varchar', length: 128, unique: true })
  username: string;

  // Только bcrypt-хэш, plain-пароль никогда не хранится и не логируется.
  @Column({ type: 'varchar', length: 256, name: 'password_hash' })
  passwordHash: string;

  @Column({ type: 'enum', enum: Role, default: Role.VIEWER })
  role: Role;

  @Column({ type: 'boolean', name: 'is_active', default: true })
  isActive: boolean;

  @CreateDateColumn({ name: 'created_at' })
  createdAt: Date;

  @UpdateDateColumn({ name: 'updated_at' })
  updatedAt: Date;
}
