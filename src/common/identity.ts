import { Signer } from "@aws-amplify/core";
import { CognitoIdentity, CognitoIdentityCredentials } from "aws-sdk";

import AmazonCognitoIdentity, { CognitoIdToken } from "amazon-cognito-identity-js";

// FIXME: Hard coded
const region = "eu-west-1";
const UserPoolId = "eu-west-1_0GLV9KO1p";
const IdentityPoolId = "eu-west-1:bce21571-e3a6-47a4-8032-fd015213405f";
const webSocketUrl = "wss://m9fhs4t5vh.execute-api.eu-west-1.amazonaws.com/dev";
const Logins = `cognito-idp.${region}.amazonaws.com/${UserPoolId}`;
const poolData = { UserPoolId, ClientId: "6timr8knllr4frovfvq8r2o6oo" };
const Pool = new AmazonCognitoIdentity.CognitoUserPool(poolData);

export interface BoilingDataCredentials {
  cognitoUsername: string;
  signedWebsocketUrl: string;
}

function getIdToken(Username: string, Password: string): Promise<CognitoIdToken> {
  return new Promise((resolve, reject) => {
    const params = { Username, Password };
    const userData = { Username, Pool };
    const cognitoUser = new AmazonCognitoIdentity.CognitoUser(userData);
    const authenticationDetails = new AmazonCognitoIdentity.AuthenticationDetails(params);
    cognitoUser.authenticateUser(authenticationDetails, {
      onSuccess: result => resolve(result?.getIdToken()),
      onFailure: err => reject(err),
    });
  });
}

async function refreshCredsWithToken(idToken: string): Promise<CognitoIdentityCredentials> {
  const creds = new CognitoIdentityCredentials(
    {
      IdentityPoolId,
      Logins: { [Logins]: idToken },
    },
    { region },
  );
  await creds.getPromise();
  return creds;
}

export async function getBoilingDataCredentials(username: string, password: string): Promise<BoilingDataCredentials> {
  const idToken = await getIdToken(username, password);
  const creds = await refreshCredsWithToken(idToken.getJwtToken());
  const params = {
    access_key: creds.data?.Credentials?.AccessKeyId,
    secret_key: (<CognitoIdentity.Types.GetCredentialsForIdentityResponse>creds.data)?.Credentials?.SecretKey,
    session_token: creds.data?.Credentials?.SessionToken,
  };
  const signedWebsocketUrl = await Signer.signUrl(webSocketUrl, params);
  const cognitoUsername = idToken.decodePayload()["cognito:username"];
  return { cognitoUsername, signedWebsocketUrl };
}
