import { createParamDecorator, ExecutionContext } from '@nestjs/common';

export interface RequestUser {
  sub: string;
  username: string;
  role: string;
}

export const CurrentUser = createParamDecorator((_: unknown, ctx: ExecutionContext): RequestUser => {
  const request = ctx.switchToHttp().getRequest();
  return request.user;
});
