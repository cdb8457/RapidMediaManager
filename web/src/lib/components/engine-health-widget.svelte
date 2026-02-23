<script lang="ts">
	import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '$lib/components/ui/card';
	import { Badge } from '$lib/components/ui/badge';
	import { onMount } from 'svelte';
	import client from '$lib/api';
	import { Server, ShieldCheck, Activity } from 'lucide-svelte';

	interface EngineHealth {
		status: string;
		real_debrid: {
			enabled: boolean;
			status: string;
			message: string;
		};
		decypharr: {
			enabled: boolean;
			status: string;
			message: string;
		};
	}

	let health: EngineHealth | null = $state(null);
	let loading = $state(true);

	onMount(async () => {
		// Custom fetch to our new endpoint
		try {
			// @ts-ignore: Endpoint not in generated OpenAPI spec yet
			const res = await client.GET('/api/v1/engine/health');
			health = res.data as EngineHealth;
		} catch (e) {
			console.error("Failed to fetch engine health", e);
		} finally {
			loading = false;
		}
	});
</script>

<div class="grid gap-4 md:grid-cols-2 lg:grid-cols-2 mb-8">
	<!-- Real-Debrid Status -->
	<Card>
		<CardHeader class="flex flex-row items-center justify-between space-y-0 pb-2">
			<CardTitle class="text-sm font-medium">Real-Debrid Engine</CardTitle>
			<Server class="h-4 w-4 text-muted-foreground" />
		</CardHeader>
		<CardContent>
			{#if loading}
				<div class="text-2xl font-bold text-muted-foreground">Checking...</div>
			{:else if health}
				<div class="flex items-center gap-2 mb-1">
					{#if health.real_debrid.status === 'online'}
						<Badge variant="default" class="bg-green-600 hover:bg-green-700">Online</Badge>
					{:else if health.real_debrid.status === 'error'}
						<Badge variant="destructive">Error</Badge>
					{:else}
						<Badge variant="secondary">Offline</Badge>
					{/if}
				</div>
				<p class="text-xs text-muted-foreground mt-2">
					{health.real_debrid.message}
				</p>
			{/if}
		</CardContent>
	</Card>

	<!-- Decypharr (Flaresolverr) Status -->
	<Card>
		<CardHeader class="flex flex-row items-center justify-between space-y-0 pb-2">
			<CardTitle class="text-sm font-medium">Decypharr Proxy</CardTitle>
			<ShieldCheck class="h-4 w-4 text-muted-foreground" />
		</CardHeader>
		<CardContent>
			{#if loading}
				<div class="text-2xl font-bold text-muted-foreground">Checking...</div>
			{:else if health}
				<div class="flex items-center gap-2 mb-1">
					{#if health.decypharr.status === 'online'}
						<Badge variant="default" class="bg-green-600 hover:bg-green-700">Online</Badge>
					{:else if health.decypharr.status === 'error'}
						<Badge variant="destructive">Error</Badge>
					{:else}
						<Badge variant="secondary">Offline</Badge>
					{/if}
				</div>
				<p class="text-xs text-muted-foreground mt-2">
					{health.decypharr.message}
				</p>
			{/if}
		</CardContent>
	</Card>
</div>
