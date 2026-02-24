<script lang="ts">
	import { Button } from '$lib/components/ui/button';
	import * as Card from '$lib/components/ui/card';
	import { Input } from '$lib/components/ui/input';
	import { Label } from '$lib/components/ui/label';
	import { Switch } from '$lib/components/ui/switch';
	import { toast } from 'svelte-sonner';
	import client from '$lib/api';
	import { onMount } from 'svelte';

	let isLoading = $state(true);
	let isSaving = $state(false);

	let rdEnabled = $state(false);
	let rdApiKey = $state('');

	let decypharrEnabled = $state(false);
	let decypharrUrl = $state('');

	let seerrUrl = $state('');
	let seerrApiKey = $state('');

	onMount(async () => {
		try {
			const { data, error } = await client.GET('/api/v1/engine/settings', {
				// Prevent browser caching the GET request on SPA navigation
				cache: 'no-store',
				headers: {
					'Cache-Control': 'no-cache',
					Pragma: 'no-cache'
				}
			});
			if (error) throw error;

			if (data) {
				rdEnabled = data.real_debrid?.enabled ?? false;
				rdApiKey = data.real_debrid?.api_key ?? '';

				decypharrEnabled = data.decypharr?.enabled ?? false;
				decypharrUrl = data.decypharr?.url ?? 'http://localhost:8191';

				seerrUrl = data.seerr?.url ?? '';
				seerrApiKey = data.seerr?.api_key ?? '';
			}
		} catch (e) {
			toast.error('Failed to load engine settings');
			console.error(e);
		} finally {
			isLoading = false;
		}
	});

	async function saveSettings() {
		isSaving = true;
		try {
			// We cast to any because the openapi schema hasn't been re-generated yet to include EngineSettings
			const payload: any = {
				real_debrid: {
					enabled: rdEnabled,
					api_key: rdApiKey
				},
				decypharr: {
					enabled: decypharrEnabled,
					url: decypharrUrl
				},
				seerr: {
					url: seerrUrl,
					api_key: seerrApiKey
				}
			};

			const { error } = await client.POST('/api/v1/engine/settings', {
				body: payload
			});

			if (error) throw error;
			toast.success(
				'Engine settings saved successfully. The backend config.dev.toml has been updated.'
			);
		} catch (e) {
			toast.error('Failed to save settings');
			console.error(e);
		} finally {
			isSaving = false;
		}
	}
</script>

<div class="grid gap-6">
	<Card.Root>
		<Card.Header>
			<div class="flex items-center justify-between">
				<div>
					<Card.Title>Real-Debrid Engine</Card.Title>
					<Card.Description
						>Configure your Real-Debrid API token for native playback and high-speed caching.</Card.Description
					>
				</div>
				<Switch bind:checked={rdEnabled} disabled={isLoading || isSaving} />
			</div>
		</Card.Header>
		<Card.Content>
			<div class="grid gap-4">
				<div class="grid gap-2">
					<Label for="rd_api_key">API Token</Label>
					<Input
						id="rd_api_key"
						type="password"
						placeholder="Paste your Real-Debrid API token here..."
						bind:value={rdApiKey}
						disabled={isLoading || isSaving || !rdEnabled}
					/>
				</div>
			</div>
		</Card.Content>
	</Card.Root>

	<Card.Root>
		<Card.Header>
			<div class="flex items-center justify-between">
				<div>
					<Card.Title>Decypharr Proxy Engine</Card.Title>
					<Card.Description
						>Configure your local Flaresolverr instance to bypass Cloudflare protection on Indexers.</Card.Description
					>
				</div>
				<Switch bind:checked={decypharrEnabled} disabled={isLoading || isSaving} />
			</div>
		</Card.Header>
		<Card.Content>
			<div class="grid gap-4">
				<div class="grid gap-2">
					<Label for="proxy_url">Proxy URL</Label>
					<Input
						id="proxy_url"
						type="url"
						placeholder="http://localhost:8191"
						bind:value={decypharrUrl}
						disabled={isLoading || isSaving || !decypharrEnabled}
					/>
				</div>
			</div>
		</Card.Content>
	</Card.Root>

	<Card.Root>
		<Card.Header>
			<Card.Title>Seerr Integration</Card.Title>
			<Card.Description>
				Connect your standalone Seerr instance to fetch active media requests.
			</Card.Description>
		</Card.Header>
		<Card.Content>
			<div class="grid gap-4">
				<div class="grid gap-2">
					<Label for="seerr_url">Seerr URL</Label>
					<Input
						id="seerr_url"
						type="url"
						placeholder="http://localhost:5055"
						bind:value={seerrUrl}
						disabled={isLoading || isSaving}
					/>
				</div>
				<div class="grid gap-2">
					<Label for="seerr_api_key">API Key</Label>
					<Input
						id="seerr_api_key"
						type="password"
						placeholder="Paste your Seerr API key here..."
						bind:value={seerrApiKey}
						disabled={isLoading || isSaving}
					/>
				</div>
			</div>
		</Card.Content>
	</Card.Root>

	<div class="flex justify-end pt-4">
		<Button onclick={saveSettings} disabled={isLoading || isSaving}>
			{isSaving ? 'Saving...' : 'Save Engine Configuration'}
		</Button>
	</div>
</div>
