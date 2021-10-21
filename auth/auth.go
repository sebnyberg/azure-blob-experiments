package auth

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/Azure/azure-sdk-for-go/profiles/2020-09-01/resources/mgmt/subscriptions"
	mgmtstorage "github.com/Azure/azure-sdk-for-go/profiles/2020-09-01/storage/mgmt/storage"
	"github.com/Azure/azure-sdk-for-go/storage"
	"github.com/Azure/go-autorest/autorest"
	"github.com/Azure/go-autorest/autorest/azure"
	"github.com/Azure/go-autorest/autorest/azure/auth"
)

// ErrAccountNotFound is returned when a storage account could not be found.
var ErrAccountNotFound = errors.New("account not found")

// getAccountClient retrieves a blob storage account client in an automated way
// based on the local Azure CLI session or SP credentials in environment vars.
//
// This function will pick the search for the first storage account name in any
// subscription available to the user that matches the requested name.
//
func getAccountKey(
	ctx context.Context,
	accountName string,
) (string, error) {
	// Initialize authorizer based either on CLI or environment variables.
	authorizer, err := auth.NewAuthorizerFromCLIWithResource(
		azure.PublicCloud.ResourceManagerEndpoint,
	)
	if err != nil { // Try environment
		cfg := &auth.ClientCredentialsConfig{
			ClientID:     os.Getenv("AZURE_CLIENT_ID"),
			ClientSecret: os.Getenv("AZURE_CLIENT_SECRET"),
			TenantID:     os.Getenv("AZURE_TENANT_ID"),
			Resource:     azure.PublicCloud.ResourceManagerEndpoint,
			AADEndpoint:  azure.PublicCloud.ActiveDirectoryEndpoint,
		}
		authorizer, err = cfg.Authorizer()
		if err != nil {
			return "", fmt.Errorf("auth initialization failed, %v", err)
		}
	}

	// Get subscription ID.
	subIDs, err := listUserSubscriptions(ctx, authorizer)
	if err != nil {
		return "", fmt.Errorf(
			"could not infer subscription id for logged in user, %v", err)
	}

	// Find the resource group name of the account
	var rg, rgSubID string
	for _, subID := range subIDs {
		subRG, err := findAccountRG(ctx, authorizer, subID, accountName)
		if err != nil && !errors.Is(err, ErrAccountNotFound) {
			return "", fmt.Errorf("failed to lookup account resource group, %v", err)
		}
		if subRG != "" {
			rg = subRG
			rgSubID = subID
			break
		}
	}
	if rg == "" {
		return "", fmt.Errorf("could not find storage account, %v", err)
	}

	// Get account key
	accountKey, err := getFirstAccountKey(ctx, authorizer, rgSubID, rg, accountName)
	if err != nil {
		return "", err
	}

	return accountKey, nil
}

// getFirstAccountKey lists account keys and picks the first one from the list.
func getFirstAccountKey(
	ctx context.Context,
	authorizer autorest.Authorizer,
	subscriptionID string,
	resourceGroupName string,
	accountName string,
) (string, error) {
	accountsClient := mgmtstorage.NewAccountsClient(subscriptionID)
	accountsClient.Authorizer = authorizer
	listKeysResult, err := accountsClient.ListKeys(ctx,
		resourceGroupName, accountName)
	if err != nil {
		return "", err
	}
	if len(*listKeysResult.Keys) == 0 {
		return "", errors.New("failed to list keys in storage acc")
	}
	key := *(*listKeysResult.Keys)[0].Value
	return key, nil
}

// findAccountRG lists storage accounts available within the subscription and
// returns the resourge group name of the first matching storage account, or an
// error if the storage account could not be found.
func findAccountRG(
	ctx context.Context,
	authorizer autorest.Authorizer,
	subscriptionID string,
	accountName string,
) (string, error) {
	accountsClient := mgmtstorage.NewAccountsClient(subscriptionID)
	accountsClient.Authorizer = authorizer
	result, err := accountsClient.List(ctx)
	if err != nil {
		return "", fmt.Errorf("list blob accounts err, %w", err)
	}
	for _, val := range *result.Value {
		if *val.Name == accountName {
			idparts := strings.Split(*val.ID, "/")
			if len(idparts) < 4 {
				return "", errors.New("invalid account id")
			}
			return idparts[4], nil
		}
	}
	return "", ErrAccountNotFound
}

// listUserSubscriptions returns a list of subscription ids available to the
// user.
func listUserSubscriptions(
	ctx context.Context,
	authorizer autorest.Authorizer,
) ([]string, error) {
	subscriptionClient := subscriptions.NewClient()
	subscriptionClient.Authorizer = authorizer
	listResult, err := subscriptionClient.List(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list subscriptions, %v", err)
	}
	subs := listResult.Values()
	res := make([]string, len(subs))
	for i, sub := range subs {
		res[i] = *sub.SubscriptionID
	}
	return res, nil
}

var (
	errBlobNotFound = errors.New("blob not found")
)

func parseAzureBlobErr(err error) error {
	if err != nil {
		azErr, ok := err.(storage.AzureStorageServiceError)
		if !ok {
			return err
		}
		switch azErr.Code {
		case "BlobNotFound":
			return errBlobNotFound
			// case "LeaseAlreadyPresent":
			// 	return ErrBlobBusy
			// case "BlockCountExceedsLimit":
			// 	return ErrBlockCountLimitReached
		}
	}
	return err
}
