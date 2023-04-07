import { HttpRequest } from "@aws-sdk/protocol-http";
import { SignatureV4 } from "@aws-sdk/signature-v4";
import { Sha256 } from "@aws-crypto/sha256-browser";
import { Credentials, Provider } from "@aws-sdk/types";

function getSigner(region: string, credentials: any): SignatureV4 {
  const service = "execute-api";
  return new SignatureV4({ credentials, region, service, sha256: Sha256 });
}

export async function getSignedWssUrl(
  hostAndOptionalPort: string,
  credentials: Credentials | Provider<Credentials>,
  region: string,
  protocol = "wss",
  path = "/dev",
): Promise<string> {
  const [host, portCandidate] = hostAndOptionalPort.split(":");
  const port = parseInt(portCandidate ?? "443");
  const request = new HttpRequest({ protocol, headers: { host }, hostname: host, path, port });
  const fiveMinsS = 5 * 60;
  const signedRequest = await getSigner(region, credentials).presign(request, { expiresIn: fiveMinsS });
  const searchParams = signedRequest.query ? Object.entries(signedRequest.query).map(k => [k[0], `${k[1]}`]) : [];
  const signatureParams = new URLSearchParams(searchParams).toString();
  return `wss://${host}${path}?${signatureParams}`;
}
