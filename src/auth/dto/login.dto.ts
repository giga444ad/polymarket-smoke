import { IsString, Length } from 'class-validator';

export class LoginDto {
  @IsString()
  @Length(1, 128)
  username: string;

  @IsString()
  @Length(1, 256)
  password: string;
}
