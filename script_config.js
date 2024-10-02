
sidebar.ALLOWED_PROPERTIES = [
    {
        key: 'STRIPE_API_KEY',
        label: 'Stripe API Key',
        type: 'password', // 'text', 'password', 'textarea', 'select', etc.
        required: true,
        tooltip: 'Your secret Stripe API key.',
        scope: 'script' // Allowed scopes: 'script'
    },
    {
        key: 'RECEIPTS_FOLDER_URL',
        label: 'Receipts Folder URL',
        type: 'url',
        required: true,
        tooltip: 'The Google Drive folder URL where receipts will be saved.',
        scope: 'document' // Allowed scopes: 'user', 'document'
    },
    {
        key: 'STRIPE_PAYOUT_DESCRIPTION_PREFIX',
        label: 'Stripe Payout Description Prefix',
        type: 'text',
        required: true,
        tooltip: 'Prefix for payout descriptions in the sheet.',
        scope: 'user' // Allowed scopes: 'user'
    },
    {
        key: 'SUMMARY_EMAIL',
        label: 'Summary Email',
        type: 'email',
        required: true,
        tooltip: 'Email address to receive summary reports.',
        scope: 'document' // Allowed scopes: 'user', 'document'
    }
    // Add more properties as needed
];
