/*
 * Copyright 2021 Google LLC
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 */

import { URLReader } from "@malloydata/malloy";
import * as vscode from "vscode";
import { randomInt } from "crypto";

export async function fetchFile(uri: string): Promise<string> {
  return (
    await vscode.workspace.openTextDocument(uri.replace(/^file:\/\//, ""))
  ).getText();
}

export class VSCodeURLReader implements URLReader {
  async readURL(url: URL): Promise<string> {
    return fetchFile(url.toString());
  }
}

const CLIENT_ID_CHARACTERS =
  "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

function randomClientIdCharacter() {
  return CLIENT_ID_CHARACTERS.charAt(
    Math.floor(randomInt(0, CLIENT_ID_CHARACTERS.length))
  );
}

export function getNewClientId(): string {
  return Array.from({ length: 32 }, randomClientIdCharacter).join("");
}
