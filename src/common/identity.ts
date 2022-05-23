import { CognitoIdentity, CognitoIdentityCredentials } from "aws-sdk";
import { CognitoIdToken, CognitoUserPool, CognitoUser, AuthenticationDetails } from "amazon-cognito-identity-js";
import { getSignedWssUrl } from "./signature";
import { BDAWSRegion } from "boilingdata/boilingdata";

// FIXME: Hard coded
const region = "eu-west-1";
const UserPoolId = "eu-west-1_0GLV9KO1p";
const Logins = `cognito-idp.${region}.amazonaws.com/${UserPoolId}`;
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
  const creds = new CognitoIdentityCredentials(idParams, { region });
  await creds.getPromise();
  return creds;
}

function getWsApiDomain(region: string): string {
  switch (region) {
    case "eu-west-1":
      return "m9fhs4t5vh.execute-api.eu-west-1.amazonaws.com";
    case "eu-north-1":
      return "ei0k349i7d.execute-api.eu-north-1.amazonaws.com";
    default:
      return "m9fhs4t5vh.execute-api.eu-west-1.amazonaws.com";
  }
}

export async function getBoilingDataCredentials(
  username: string,
  password: string,
  region: BDAWSRegion = "eu-west-1",
): Promise<BDCredentials> {
  const webSocketHost = getWsApiDomain(region);
  const idToken = await getIdToken(username, password);
  const creds = await refreshCredsWithToken(idToken.getJwtToken());
  const accessKeyId = creds.data?.Credentials?.AccessKeyId;
  const secretAccessKey = (<CognitoIdentity.Types.GetCredentialsForIdentityResponse>creds.data)?.Credentials?.SecretKey;
  const sessionToken = creds.data?.Credentials?.SessionToken;
  if (!accessKeyId || !secretAccessKey) throw new Error("Missing credentials (after refresh)!");
  const credentials = { accessKeyId, secretAccessKey, sessionToken };
  const signedWebsocketUrl = await getSignedWssUrl(webSocketHost, credentials, region);
  const cognitoUsername = idToken.decodePayload()["cognito:username"];
  return { cognitoUsername, signedWebsocketUrl };
}
