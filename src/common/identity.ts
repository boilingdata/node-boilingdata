import { CognitoIdentity, CognitoIdentityCredentials } from "aws-sdk";
import { CognitoIdToken, CognitoUserPool, CognitoUser, AuthenticationDetails } from "amazon-cognito-identity-js";
import { getSignedWssUrl } from "./signature";

// FIXME: Hard coded
const region = "eu-west-1";
const UserPoolId = "eu-west-1_0GLV9KO1p";
const IdentityPoolId = "eu-west-1:bce21571-e3a6-47a4-8032-fd015213405f";
const Logins = `cognito-idp.${region}.amazonaws.com/${UserPoolId}`;
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

export async function getBoilingDataCredentials(
  username: string,
  password: string,
  region: string,
): Promise<BDCredentials> {
  const idToken = await getIdToken(username, password);
  const creds = await refreshCredsWithToken(idToken.getJwtToken());
  const accessKeyId = creds.data?.Credentials?.AccessKeyId;
  const secretAccessKey = (<CognitoIdentity.Types.GetCredentialsForIdentityResponse>creds.data)?.Credentials?.SecretKey;
  const sessionToken = creds.data?.Credentials?.SessionToken;
  if (!accessKeyId || !secretAccessKey) throw new Error("Missing credentials (after refresh)!");
  const credentials = { accessKeyId, secretAccessKey, sessionToken };
  const path = "/dev";
  const protocol = "wss";
  const webSocketHost = `${region}.api.boilingdata.com`
  const signedWebsocketUrl = await getSignedWssUrl(webSocketHost, credentials, protocol, path, region);
  const cognitoUsername = idToken.decodePayload()["cognito:username"];
  return { cognitoUsername, signedWebsocketUrl };
}
