/*
 * Copyright 2022 Google LLC
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

import * as vscode from "vscode";
import { MALLOY_EXTENSION_STATE } from "./state";
import fetch from "node-fetch";

const telemetryLog = vscode.window.createOutputChannel("Malloy Telemetry");

function isTelemetryEnabled() {
  const vsCodeValue = vscode.env.isTelemetryEnabled;
  const configValue =
    vscode.workspace.getConfiguration("malloy").get("telemetry") ?? false;
  return vsCodeValue && configValue;
}

export interface GATrackingEvent {
  name: string;
  params: Record<string, string>;
}

// TODO get these from somewhere
const MEASUREMENT_ID = "";
const API_SECRET = "";

async function track(event: GATrackingEvent) {
  if (!isTelemetryEnabled()) return;

  telemetryLog.appendLine(`Logging telemetry event: ${event}.`);

  try {
    await fetch(
      `https://www.google-analytics.com/mp/collect?measurement_id=${MEASUREMENT_ID}&api_secret=${API_SECRET}`,
      {
        method: "POST",
        body: JSON.stringify({
          client_id: MALLOY_EXTENSION_STATE.getClientId(),
          events: [event],
        }),
      }
    );
  } catch (error) {
    telemetryLog.appendLine(`Logging telemetry event failed: ${error}`);
  }
}

export function trackQueryRun(): Promise<void> {
  return track({
    name: "query_run",
    params: {},
  });
}
