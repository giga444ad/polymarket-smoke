import { SetMetadata } from '@nestjs/common';

// Помечает ручку как не требующую JWT (используется ТОЛЬКО для /health и,
// на IS_MAIN, для /auth/login — см. AuthModule).
export const IS_PUBLIC_KEY = 'isPublic';
export const Public = () => SetMetadata(IS_PUBLIC_KEY, true);
