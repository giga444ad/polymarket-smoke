/**
 * Ручное создание/обновление пользователя — единственный способ завести
 * логин (регистрации через API нет и не планируется, см. CONTEXT.md).
 * Запускать ТОЛЬКО на IS_MAIN-сервисе (только там есть таблица users,
 * которая реально используется для логина).
 *
 * Использование:
 *   npx tsx scripts/create-user.ts <username> <password> <admin|viewer>
 *
 * Идемпотентно: если username уже существует — обновит пароль и роль.
 */
import 'reflect-metadata';
import * as bcrypt from 'bcrypt';
import { DataSource } from 'typeorm';
import { User } from '../src/auth/user.entity';
import { Role } from '../src/auth/role.enum';

async function main() {
  const [username, password, roleArg] = process.argv.slice(2);
  if (!username || !password || !roleArg) {
    console.error('Использование: npx tsx scripts/create-user.ts <username> <password> <admin|viewer>');
    process.exit(1);
  }
  if (!Object.values(Role).includes(roleArg as Role)) {
    console.error(`Роль должна быть одной из: ${Object.values(Role).join(', ')}`);
    process.exit(1);
  }
  if (password.length < 12) {
    console.error('Пароль должен быть не короче 12 символов.');
    process.exit(1);
  }

  const dataSource = new DataSource({
    type: 'postgres',
    host: process.env.POSTGRES_HOST || 'localhost',
    port: parseInt(process.env.POSTGRES_PORT || '5432', 10),
    username: process.env.POSTGRES_USER || 'postgres',
    password: process.env.POSTGRES_PASSWORD || 'postgres',
    database: process.env.POSTGRES_DB || 'polymarket_bot',
    entities: [User],
  });
  await dataSource.initialize();

  const repo = dataSource.getRepository(User);
  const passwordHash = await bcrypt.hash(password, 12);

  let user = await repo.findOne({ where: { username } });
  if (user) {
    user.passwordHash = passwordHash;
    user.role = roleArg as Role;
    user.isActive = true;
    await repo.save(user);
    console.log(`Обновлён пользователь "${username}" (роль: ${roleArg}).`);
  } else {
    user = repo.create({ username, passwordHash, role: roleArg as Role, isActive: true });
    await repo.save(user);
    console.log(`Создан пользователь "${username}" (роль: ${roleArg}).`);
  }

  await dataSource.destroy();
}

main().catch((err) => {
  console.error('Ошибка:', err);
  process.exit(1);
});
