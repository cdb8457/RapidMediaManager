<script lang="ts">
	import * as Table from '$lib/components/ui/table';
	import { Button } from '$lib/components/ui/button';
	import { Trash2, RefreshCw } from 'lucide-svelte';
	import { toast } from 'svelte-sonner';
	import client from '$lib/api';
	import { onMount } from 'svelte';
	import * as Sidebar from '$lib/components/ui/sidebar/index.js';
	import { Separator } from '$lib/components/ui/separator';
	import * as Breadcrumb from '$lib/components/ui/breadcrumb/index.js';
	import { resolve } from '$app/paths';

	let torrents: any[] = $state([]);
	let isLoading = $state(true);
	let isDeleting = $state(false);

	async function loadTorrents() {
		isLoading = true;
		try {
			const { data, error } = await client.GET('/api/v1/engine/torrents', {});
			if (error) throw error;
			torrents = data || [];
		} catch (e) {
			toast.error('Failed to load Debrid cache');
			console.error(e);
		} finally {
			isLoading = false;
		}
	}

	onMount(() => {
		loadTorrents();
	});

	async function deleteTorrent(id: string) {
		if (isDeleting) return;
		isDeleting = true;
		try {
			const { error } = await client.DELETE(`/api/v1/engine/torrents/${id}` as any, {});
			if (error) throw error;
			toast.success('Torrent deleted from Real-Debrid cache.');
			loadTorrents(); // refresh the list
		} catch (e) {
			toast.error('Failed to delete torrent');
			console.error(e);
		} finally {
			isDeleting = false;
		}
	}

	function formatBytes(bytes: number, decimals = 2) {
		if (!+bytes) return '0 Bytes';
		const k = 1024;
		const dm = decimals < 0 ? 0 : decimals;
		const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB'];
		const i = Math.floor(Math.log(bytes) / Math.log(k));
		return `${parseFloat((bytes / Math.pow(k, i)).toFixed(dm))} ${sizes[i]}`;
	}
</script>

<svelte:head>
	<title>Debrid Cache - MediaManager</title>
	<meta
		content="Manage your Real-Debrid active transfers and full torrent cache"
		name="description"
	/>
</svelte:head>

<header class="flex h-16 shrink-0 items-center gap-2">
	<div class="flex items-center gap-2 px-4">
		<Sidebar.Trigger class="-ml-1" />
		<Separator class="mr-2 h-4" orientation="vertical" />
		<Breadcrumb.Root>
			<Breadcrumb.List>
				<Breadcrumb.Item class="hidden md:block">
					<Breadcrumb.Link href={resolve('/dashboard', {})}>MediaManager</Breadcrumb.Link>
				</Breadcrumb.Item>
				<Breadcrumb.Separator class="hidden md:block" />
				<Breadcrumb.Item>
					<Breadcrumb.Page>Debrid Cache Management</Breadcrumb.Page>
				</Breadcrumb.Item>
			</Breadcrumb.List>
		</Breadcrumb.Root>
	</div>
</header>

<main class="mx-auto flex w-full flex-1 flex-col gap-4 p-4 md:max-w-[80em]">
	<div class="my-6 flex items-center justify-between">
		<h1 class="scroll-m-20 text-4xl font-extrabold tracking-tight lg:text-5xl">Debrid Cache</h1>
		<Button variant="outline" size="icon" onclick={loadTorrents} disabled={isLoading}>
			<RefreshCw class="h-4 w-4" />
			<span class="sr-only">Refresh</span>
		</Button>
	</div>

	<div class="overflow-hidden rounded-md border bg-card text-card-foreground shadow-sm">
		<div class="border-b p-4">
			<h3 class="leading-none font-semibold tracking-tight">Real-Debrid History</h3>
			<p class="mt-2 text-sm text-muted-foreground">
				Manage all historic torrents stored in your Debrid account. Deleting them here will
				permanently remove them from the Real-Debrid network.
			</p>
		</div>
		<Table.Root>
			<Table.Header>
				<Table.Row>
					<Table.Head>Name</Table.Head>
					<Table.Head>Status</Table.Head>
					<Table.Head>Progress</Table.Head>
					<Table.Head>Speed</Table.Head>
					<Table.Head class="text-right">Actions</Table.Head>
				</Table.Row>
			</Table.Header>
			<Table.Body>
				{#if isLoading}
					<Table.Row>
						<Table.Cell colspan={5} class="py-8 text-center text-muted-foreground"
							>Loading cache history...</Table.Cell
						>
					</Table.Row>
				{:else if torrents.length === 0}
					<Table.Row>
						<Table.Cell colspan={5} class="py-8 text-center text-muted-foreground"
							>No torrents found in Debrid account.</Table.Cell
						>
					</Table.Row>
				{:else}
					{#each torrents as torrent}
						<Table.Row>
							<Table.Cell class="max-w-[300px] truncate font-medium" title={torrent.title}
								>{torrent.title}</Table.Cell
							>
							<Table.Cell>
								<div
									class="inline-flex items-center rounded-full border px-2.5 py-0.5 text-xs font-semibold ring-offset-background transition-colors focus:ring-2 focus:ring-ring focus:ring-offset-2 focus:outline-none
									{torrent.status === 'downloaded'
										? 'border-transparent bg-primary text-primary-foreground'
										: torrent.status === 'error' || torrent.status === 'dead'
											? 'border-transparent bg-destructive text-destructive-foreground'
											: 'border-transparent bg-secondary text-secondary-foreground'}"
								>
									{torrent.status}
								</div>
							</Table.Cell>
							<Table.Cell>{torrent.progress}</Table.Cell>
							<Table.Cell>{torrent.speed ? formatBytes(torrent.speed) + '/s' : '-'}</Table.Cell>
							<Table.Cell class="text-right">
								<Button
									variant="ghost"
									size="icon"
									onclick={() => deleteTorrent(torrent.id)}
									disabled={isDeleting}
								>
									<Trash2 class="h-4 w-4 text-destructive" />
									<span class="sr-only">Delete</span>
								</Button>
							</Table.Cell>
						</Table.Row>
					{/each}
				{/if}
			</Table.Body>
		</Table.Root>
	</div>
</main>
