import { CognitoIdentity, CognitoIdentityCredentials } from "aws-sdk";
import { CognitoIdToken, CognitoUserPool, CognitoUser, AuthenticationDetails } from "amazon-cognito-identity-js";
import { getSignedWssUrl } from "./signature";
import { BDAWSRegion } from "boilingdata/boilingdata";

const IDP_REGION = "eu-west-1";
const UserPoolId = "eu-west-1_0GLV9KO1p";
const Logins = `cognito-idp.${IDP_REGION}.amazonaws.com/${UserPoolId}`;
const IdentityPoolId = "eu-west-1:bce21571-e3a6-47a4-8032-fd015213405f";
const poolData = { UserPoolId, ClientId: "6timr8knllr4frovfvq8r2o6oo" };
const Pool = new CognitoUserPool(poolData);

export interface BDCredentials {
  cognitoUsername: string;
  signedWebsocketUrl: string;
}

function getIdToken(Username: string, Password: string): Promise<CognitoIdToken> {
  return new Promise((resolve, reject) => {
    const loginDetails = { Username, Password };
    const userData = { Username, Pool };
    const cognitoUser = new CognitoUser(userData);
    const authenticationDetails = new AuthenticationDetails(loginDetails);
    cognitoUser.authenticateUser(authenticationDetails, {
      onSuccess: (result: any) => resolve(result?.getIdToken()),
      onFailure: (err: any) => reject(err),
    });
  });
}

async function refreshCredsWithToken(idToken: string): Promise<CognitoIdentityCredentials> {
  const idParams = { IdentityPoolId, Logins: { [Logins]: idToken } };
  const creds = new CognitoIdentityCredentials(idParams, { region: IDP_REGION });
  await creds.getPromise();
  return creds;
}

function getWsApiDomain(region: string, endpointUrl?: string): string {
  return endpointUrl ? endpointUrl : `${region}.api.boilingdata.com`;
  // return `api.boilingdata.com`;
}

export async function getBoilingDataCredentials(
  username: string,
  password: string,
  region: BDAWSRegion = "eu-west-1",
  endpointUrl?: string,
): Promise<BDCredentials> {
  const webSocketHost = getWsApiDomain(region, endpointUrl);
  const idToken = await getIdToken(username, password);
  const creds = await refreshCredsWithToken(idToken.getJwtToken());
  const accessKeyId = creds.data?.Credentials?.AccessKeyId;
  const secretAccessKey = (<CognitoIdentity.Types.GetCredentialsForIdentityResponse>creds.data)?.Credentials?.SecretKey;
  const sessionToken = creds.data?.Credentials?.SessionToken;
  if (!accessKeyId || !secretAccessKey) throw new Error("Missing credentials (after refresh)!");
  const credentials = { accessKeyId, secretAccessKey, sessionToken };
  const signedWebsocketUrl = await getSignedWssUrl(webSocketHost, credentials, region, "wss", "");
  const cognitoUsername = idToken.decodePayload()["cognito:username"];
  return { cognitoUsername, signedWebsocketUrl };
}
